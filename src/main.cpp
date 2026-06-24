// ============================================================
// ESP32 BLE -> MQTT Gateway
//
// platformio.ini lib_deps:
//   h2zero/NimBLE-Arduino@^1.4.1
//   knolleary/PubSubClient@^2.8
//   ESP32Async/ESPAsyncWebServer@^3.0.0
//   ESP32Async/AsyncTCP@^3.0.0
// ============================================================

#include <Arduino.h>
#include <WiFi.h>
#include <DNSServer.h>
#include <ESPAsyncWebServer.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <NimBLEDevice.h>
#include <time.h>
#include <ctype.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "esp_system.h"

// ============================================================
// TUNABLES
// ============================================================
#define MAX_TARGET_MACS           8
#define MAC_STR_LEN              13    // 12 hex chars + null
#define MAX_RAW_HEX_LEN          63    // 31 payload bytes → 62 hex chars + null
#define QUEUE_LENGTH             40
#define BLE_SCAN_SECONDS          5    // duration of each scan window
#define BLE_SCAN_REST_MS        200    // pause between consecutive scans
#define WIFI_RECONNECT_MS     10000
#define MQTT_RECONNECT_MS      5000
#define HEAP_CHECK_MS          30000
#define HEAP_CRITICAL_BYTES    20000
#define PREVENTIVE_REBOOT_HOURS   0   // 0 = disabled
#define THROTTLE_CACHE_SIZE      16   // max unique MACs tracked for publish throttle

// ============================================================
// GLOBAL STATE
// ============================================================
AsyncWebServer server(80);
DNSServer      dnsServer;
Preferences    preferences;
WiFiClient     espClient;
PubSubClient   mqttClient(espClient);
NimBLEScan*    pBLEScan = nullptr;

// Built once in setup() during AP scan — never rebuilt in an async handler.
String g_ssidOptions = "";

struct Config {
  // MQTT
  char     mqtt_server[64];
  uint16_t mqtt_port;
  char     mqtt_user[32];
  char     mqtt_pass[32];
  char     mqtt_topic[64];
  uint8_t  mqtt_qos;
  bool     mqtt_retain;
  // BLE filter
  char     target_macs[MAX_TARGET_MACS][MAC_STR_LEN];
  uint8_t  target_mac_count;
  // BLE scan timing (stored & presented in ms, converted to NimBLE units at runtime)
  // 1 NimBLE unit = 0.625 ms  →  units = ms × 8 / 5
  // Defaults: 63 ms ≈ 100 units  |  62 ms ≈ 99 units  (≈ 100 % duty cycle)
  uint16_t ble_scan_interval_ms;
  uint16_t ble_scan_window_ms;
  // Publish throttle: minimum gap between MQTT publishes for the same MAC.
  // 0 = send every received packet immediately.
  uint16_t publish_interval_ms;
} config;

char g_ssid[33];
char g_pass[65];

struct SensorPacket {
  char   mac[MAC_STR_LEN];
  int8_t rssi;
  char   rawData[MAX_RAW_HEX_LEN];
  char   timestamp[26];
};

// Per-MAC publish throttle cache. Only ever touched from loop() on core 1
// so no mutex is needed.
struct ThrottleEntry {
  char          mac[MAC_STR_LEN];
  unsigned long lastMs;
};
ThrottleEntry g_throttleCache[THROTTLE_CACHE_SIZE];
uint8_t       g_throttleCacheCount = 0;

QueueHandle_t      sensorQueue;
volatile uint32_t  g_droppedPackets   = 0;  // BLE queue full — packet never enqueued
volatile uint32_t  g_throttledPackets = 0;  // dequeued but suppressed by publish throttle
volatile uint32_t  g_publishedPackets = 0;
volatile unsigned long g_lastPublishMs = 0;

unsigned long g_lastWifiAttempt = 0;
unsigned long g_lastMqttAttempt = 0;
unsigned long g_lastHeapCheck   = 0;
unsigned long g_bootMs          = 0;
bool          g_configMode      = false;

volatile bool          g_pendingRestart = false;
volatile unsigned long g_restartAtMs    = 0;

void scheduleRestart(unsigned long delayMs) {
  if (pBLEScan != nullptr) pBLEScan->stop();  // stop BLE cleanly before reboot
  g_pendingRestart = true;
  g_restartAtMs    = millis() + delayMs;
}

// ============================================================
// HELPERS
// ============================================================
void macBytesToStr(const uint8_t* addr, char* out) {
  // NimBLE stores BLE address bytes in LSB-first order (addr[0] = LSB).
  // Human-readable MAC notation (Minew app, nRF Connect) is MSB-first,
  // so we reverse here to keep filter comparison straightforward.
  snprintf(out, MAC_STR_LEN, "%02X%02X%02X%02X%02X%02X",
           addr[5], addr[4], addr[3], addr[2], addr[1], addr[0]);
}

void bytesToHexStr(const uint8_t* data, size_t len, char* out, size_t outSize) {
  size_t max = (outSize - 1) / 2;
  if (len > max) len = max;
  for (size_t i = 0; i < len; i++) snprintf(out + i * 2, 3, "%02X", data[i]);
  out[len * 2] = '\0';
}

void getIso8601Time(char* out, size_t outSize) {
  struct tm ti;
  if (!getLocalTime(&ti, 10)) { snprintf(out, outSize, "1970-01-01T00:00:00.000Z"); return; }
  strftime(out, outSize, "%Y-%m-%dT%H:%M:%S.000Z", &ti);
}

bool macMatchesFilter(const char* mac) {
  if (config.target_mac_count == 0) return true;
  for (uint8_t i = 0; i < config.target_mac_count; i++)
    if (strcmp(config.target_macs[i], mac) == 0) return true;
  return false;
}

// Returns true if we should forward this MAC's packet to MQTT right now.
// First appearance: always publish (adds entry to cache).
// Subsequent: only if publish_interval_ms has elapsed since last publish.
bool shouldPublish(const char* mac) {
  if (config.publish_interval_ms == 0) return true;
  unsigned long now = millis();
  for (uint8_t i = 0; i < g_throttleCacheCount; i++) {
    if (strcmp(g_throttleCache[i].mac, mac) == 0) {
      if (now - g_throttleCache[i].lastMs >= config.publish_interval_ms) {
        g_throttleCache[i].lastMs = now;
        return true;
      }
      return false;
    }
  }
  // New MAC — add to cache and publish this first packet immediately.
  if (g_throttleCacheCount < THROTTLE_CACHE_SIZE) {
    strncpy(g_throttleCache[g_throttleCacheCount].mac, mac, MAC_STR_LEN);
    g_throttleCache[g_throttleCacheCount].lastMs = now;
    g_throttleCacheCount++;
  }
  return true;
}

void parseTargetMacs(const char* input) {
  config.target_mac_count = 0;
  char buf[160];
  strncpy(buf, input, sizeof(buf) - 1);
  buf[sizeof(buf) - 1] = '\0';
  char* tok = strtok(buf, ",");
  while (tok && config.target_mac_count < MAX_TARGET_MACS) {
    while (*tok == ' ') tok++;
    char clean[MAC_STR_LEN] = {0};
    uint8_t ci = 0;
    for (uint8_t i = 0; tok[i] && ci < MAC_STR_LEN - 1; i++) {
      char c = toupper((unsigned char)tok[i]);
      if (isxdigit((unsigned char)c)) clean[ci++] = c;
    }
    clean[ci] = '\0';
    if (ci == 12) strncpy(config.target_macs[config.target_mac_count++], clean, MAC_STR_LEN);
    tok = strtok(nullptr, ",");
  }
}

void loadConfig() {
  preferences.begin("config", true);
  strncpy(g_ssid, preferences.getString("ssid",        "").c_str(), sizeof(g_ssid) - 1);
  strncpy(g_pass, preferences.getString("pass",        "").c_str(), sizeof(g_pass) - 1);
  strncpy(config.mqtt_server, preferences.getString("mqtt_server", "").c_str(),          sizeof(config.mqtt_server) - 1);
  strncpy(config.mqtt_user,   preferences.getString("mqtt_user",   "").c_str(),          sizeof(config.mqtt_user)   - 1);
  strncpy(config.mqtt_pass,   preferences.getString("mqtt_pass",   "").c_str(),          sizeof(config.mqtt_pass)   - 1);
  strncpy(config.mqtt_topic,  preferences.getString("mqtt_topic",  "esp32/sensors/").c_str(), sizeof(config.mqtt_topic) - 1);
  config.mqtt_port   = preferences.getUShort("mqtt_port",    1883);
  config.mqtt_qos    = preferences.getUChar ("mqtt_qos",       0);
  config.mqtt_retain = preferences.getBool  ("mqtt_retain", false);
  parseTargetMacs(preferences.getString("ble_mac", "").c_str());
  // BLE scan timing
  config.ble_scan_interval_ms = preferences.getUShort("scan_iv_ms",  63);
  config.ble_scan_window_ms   = preferences.getUShort("scan_win_ms", 62);
  // Publish throttle
  config.publish_interval_ms  = preferences.getUShort("pub_iv_ms",    0);
  preferences.end();
}

// ============================================================
// BLE SCANNING — NimBLE, pinned to core 0
// ============================================================
class MyScanCallbacks : public NimBLEAdvertisedDeviceCallbacks {
  void onResult(NimBLEAdvertisedDevice* dev) override {
    if (!dev->haveServiceData()) return;

    SensorPacket pkt;
    macBytesToStr(dev->getAddress().getNative(), pkt.mac);

    // With no filter active, log every seen MAC to Serial so you can
    // copy-paste the exact string into the MAC filter field on the config page.
    if (config.target_mac_count == 0)
      Serial.printf("[BLE] seen MAC: %s  RSSI: %d\n", pkt.mac, dev->getRSSI());

    if (!macMatchesFilter(pkt.mac)) return;

    pkt.rssi = (int8_t)dev->getRSSI();
    bytesToHexStr(dev->getPayload(), dev->getPayloadLength(), pkt.rawData, sizeof(pkt.rawData));
    getIso8601Time(pkt.timestamp, sizeof(pkt.timestamp));

    if (xQueueSend(sensorQueue, &pkt, 0) != pdTRUE) g_droppedPackets++;
  }
};

void bleTask(void* param) {
  NimBLEDevice::init("");
  pBLEScan = NimBLEDevice::getScan();
  pBLEScan->setAdvertisedDeviceCallbacks(new MyScanCallbacks(), true);
  pBLEScan->setActiveScan(true);

  // Convert ms → NimBLE units (1 unit = 0.625 ms → units = ms × 8 / 5).
  // Clamp window to [4 units, interval].
  uint16_t iv  = max((uint16_t)4, (uint16_t)(config.ble_scan_interval_ms * 8 / 5));
  uint16_t win = max((uint16_t)4, (uint16_t)(config.ble_scan_window_ms   * 8 / 5));
  if (win > iv) win = iv;
  pBLEScan->setInterval(iv);
  pBLEScan->setWindow(win);
  Serial.printf("[BLE] interval=%u units (%u ms)  window=%u units (%u ms)\n",
                iv, config.ble_scan_interval_ms, win, config.ble_scan_window_ms);

  for (;;) {
    pBLEScan->start(BLE_SCAN_SECONDS, false);
    pBLEScan->clearResults();
    vTaskDelay(pdMS_TO_TICKS(BLE_SCAN_REST_MS));
  }
}

// ============================================================
// NETWORKING — core 1 (loop)
// ============================================================
void ensureWifiConnected() {
  if (WiFi.status() == WL_CONNECTED) return;
  unsigned long now = millis();
  if (now - g_lastWifiAttempt < WIFI_RECONNECT_MS) return;
  g_lastWifiAttempt = now;
  Serial.println("[WiFi] Reconnecting...");
  WiFi.disconnect();
  WiFi.begin(g_ssid, g_pass);
}

void ensureMqttConnected() {
  if (mqttClient.connected() || strlen(config.mqtt_server) == 0) return;
  unsigned long now = millis();
  if (now - g_lastMqttAttempt < MQTT_RECONNECT_MS) return;
  g_lastMqttAttempt = now;
  char cid[32];
  snprintf(cid, sizeof(cid), "ESP32-GW-%04X", (unsigned int)random(0xffff));
  bool ok = strlen(config.mqtt_user) > 0
    ? mqttClient.connect(cid, config.mqtt_user, config.mqtt_pass)
    : mqttClient.connect(cid);
  Serial.printf("[MQTT] Connect %s\n", ok ? "OK" : "FAILED");
}

void drainQueueToMqtt() {
  SensorPacket pkt;
  while (mqttClient.connected() && xQueueReceive(sensorQueue, &pkt, 0) == pdTRUE) {
    if (!shouldPublish(pkt.mac)) { g_throttledPackets++; continue; }
    char payload[200];
    snprintf(payload, sizeof(payload),
      "{\"timestamp\":\"%s\",\"mac\":\"%s\",\"rssi\":%d,\"rawData\":\"%s\"}",
      pkt.timestamp, pkt.mac, pkt.rssi, pkt.rawData);
    if (mqttClient.publish(config.mqtt_topic, payload, config.mqtt_retain)) {
      g_publishedPackets++;
      g_lastPublishMs = millis();
    }
  }
}

void checkHeapHealth() {
  unsigned long now = millis();
  if (now - g_lastHeapCheck < HEAP_CHECK_MS) return;
  g_lastHeapCheck = now;
  uint32_t fh = esp_get_free_heap_size();
  Serial.printf("[Health] free=%u min=%u queue=%d pub=%u drop=%u throttle=%u\n",
    fh, esp_get_minimum_free_heap_size(),
    uxQueueMessagesWaiting(sensorQueue),
    g_publishedPackets, g_droppedPackets, g_throttledPackets);
  if (fh < HEAP_CRITICAL_BYTES) { Serial.println("[Health] CRITICAL heap."); scheduleRestart(200); }
#if PREVENTIVE_REBOOT_HOURS > 0
  if (now - g_bootMs > (unsigned long)PREVENTIVE_REBOOT_HOURS * 3600000UL) {
    Serial.println("[Health] Preventive reboot."); scheduleRestart(200);
  }
#endif
}

// ============================================================
// WEB UI
// ============================================================
String htmlHeader(const char* title, const char* extra = "") {
  String h = F("<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
    "<meta name='viewport' content='width=device-width,initial-scale=1,maximum-scale=1'>"
    "<title>"); h += title; h += F("</title>"); h += extra;
  h += F("<style>"
    ":root{--bg:#0f172a;--card:#1e293b;--text:#e2e8f0;--muted:#94a3b8;"
    "--accent:#38bdf8;--ax:#0284c7;--ok:#4ade80;--bad:#f87171;--warn:#fb923c;--bd:#334155}"
    "*{box-sizing:border-box}"
    "body{margin:0;padding:24px 16px;background:var(--bg);color:var(--text);"
    "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
    "display:flex;justify-content:center}"
    ".wrap{width:100%;max-width:500px}"
    "h2{font-size:1.4rem;margin:0 0 4px}"
    ".sub{color:var(--muted);margin:0 0 20px;font-size:.9rem}"
    ".card{background:var(--card);border:1px solid var(--bd);border-radius:14px;"
    "padding:20px;margin-bottom:16px;box-shadow:0 4px 16px rgba(0,0,0,.25)}"
    ".card h3{margin:0 0 14px;font-size:.95rem;color:var(--accent);text-transform:uppercase;letter-spacing:.05em}"
    "label{display:block;font-size:.82rem;color:var(--muted);margin:14px 0 5px}"
    "label:first-of-type{margin-top:0}"
    "input[type=text],input[type=password],input[type=number],select{"
    "width:100%;padding:11px 13px;border-radius:10px;border:1px solid var(--bd);"
    "background:#0f172a;color:var(--text);font-size:1rem;outline:none;transition:border-color .15s}"
    "input:focus,select:focus{border-color:var(--accent)}"
    ".row{display:flex;gap:12px}.row>div{flex:1}"
    ".hint{font-size:.78rem;color:var(--muted);margin:6px 0 0;line-height:1.4}"
    ".hint b{color:var(--warn)}"
    "button,input[type=submit]{width:100%;padding:13px;margin-top:18px;border:none;"
    "border-radius:10px;background:var(--accent);color:#04111f;font-weight:700;"
    "font-size:1rem;cursor:pointer;transition:background .15s}"
    "button:hover,input[type=submit]:hover{background:var(--ax);color:#fff}"
    ".danger{background:var(--bad);color:#1a0505}.danger:hover{background:#dc2626;color:#fff}"
    ".grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}"
    ".stat{background:#0f172a;border:1px solid var(--bd);border-radius:10px;padding:10px 12px}"
    ".lbl{font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}"
    ".val{font-size:1.05rem;font-weight:600;margin-top:3px}"
    ".badge{display:inline-block;padding:2px 9px;border-radius:999px;font-size:.75rem;font-weight:700}"
    ".ok{background:rgba(74,222,128,.15);color:var(--ok)}"
    ".bad{background:rgba(248,113,113,.15);color:var(--bad)}"
    "a{color:var(--accent);text-decoration:none;font-size:.85rem}"
    ".links{display:flex;justify-content:space-between;margin-top:10px}"
    "hr{border:none;border-top:1px solid var(--bd);margin:18px 0}"
  "</style></head><body><div class='wrap'>");
  return h;
}
String htmlFooter() { return F("</div></body></html>"); }

// ---- AP setup page ----
String getWifiSetupHTML() {
  String h = htmlHeader("WiFi Setup");
  h += F("<h2>ESP32 Gateway</h2><p class='sub'>Connect to your WiFi network to get started</p>"
         "<div class='card'><form action='/savewifi' method='POST'>"
         "<label>Network name (SSID)</label>"
         "<input type='text' name='ssid' list='sl' autocomplete='off' required>"
         "<datalist id='sl'>");
  h += g_ssidOptions;
  h += F("</datalist>"
         "<label>Password</label>"
         "<input type='password' name='pass' required>"
         "<input type='submit' value='Connect &amp; save'>"
         "</form></div>");
  h += htmlFooter();
  return h;
}

// ---- Main config page ----
String getMQTTSetupHTML() {
  String h = htmlHeader("Gateway Config");
  h += F("<h2>BLE &rarr; MQTT Gateway</h2><p class='sub'>Sensor gateway configuration</p>");

  // --- Status mini-card ---
  bool wOk = WiFi.status() == WL_CONNECTED, mOk = mqttClient.connected();
  unsigned long up = (millis() - g_bootMs) / 1000;
  auto badge = [](bool ok, const char* y, const char* n) {
    return String("<span class='badge ") + (ok?"ok'>":"bad'>") + (ok?y:n) + "</span>";
  };
  h += F("<div class='card'><h3>Status</h3><div class='grid'>");
  h += "<div class='stat'><div class='lbl'>WiFi</div><div class='val'>"    + badge(wOk,"Connected","Offline") + "</div></div>";
  h += "<div class='stat'><div class='lbl'>MQTT</div><div class='val'>"    + badge(mOk,"Connected","Offline") + "</div></div>";
  h += "<div class='stat'><div class='lbl'>Uptime</div><div class='val'>"  + String(up/3600) + "h " + String((up%3600)/60) + "m</div></div>";
  h += "<div class='stat'><div class='lbl'>Free heap</div><div class='val'>" + String(esp_get_free_heap_size()/1024) + " KB</div></div>";
  h += "<div class='stat'><div class='lbl'>Published</div><div class='val'>" + String(g_publishedPackets) + "</div></div>";
  h += "<div class='stat'><div class='lbl'>Throttled</div><div class='val'>" + String(g_throttledPackets) + "</div></div>";
  h += F("</div><div class='links'><a href='/status'>Full diagnostics &rarr;</a><a href='/'>Refresh</a></div></div>");

  // --- One big form for all settings ---
  h += F("<div class='card'><form action='/savemqtt' method='POST'>"
         "<h3>MQTT broker</h3>"
         "<label>Broker address</label>");
  h += "<input type='text' name='mqtt_server' value='" + String(config.mqtt_server) + "' placeholder='192.168.1.10' required>";
  h += F("<div class='row'>"
         "<div><label>Port</label>");
  h += "<input type='number' name='mqtt_port' value='" + String(config.mqtt_port) + "' required></div>";
  h += F("<div><label>QoS</label><select name='mqtt_qos'>");
  h += "<option value='0'" + String(config.mqtt_qos==0?" selected":"") + ">0 — At most once</option>";
  h += "<option value='1'" + String(config.mqtt_qos==1?" selected":"") + ">1 — At least once</option>";
  h += F("</select></div></div>"
         "<label>Username (optional)</label>");
  h += "<input type='text'     name='mqtt_user'  value='" + String(config.mqtt_user) + "'>";
  h += F("<label>Password (optional)</label>");
  h += "<input type='password' name='mqtt_pass'  value='" + String(config.mqtt_pass) + "'>";
  h += F("<label>Topic</label>");
  h += "<input type='text'     name='mqtt_topic' value='" + String(config.mqtt_topic) + "' required>";
  h += F("<label>Retain</label><select name='mqtt_retain'>");
  h += "<option value='false'" + String(!config.mqtt_retain?" selected":"") + ">False</option>";
  h += "<option value='true'"  + String( config.mqtt_retain?" selected":"") + ">True</option>";
  h += F("</select>");

  // --- Publish throttle ---
  h += F("<hr><h3>Publish rate</h3>"
         "<label>Minimum interval between MQTT publishes <i>per sensor</i></label>"
         "<select name='pub_iv_ms'>");
  const struct { uint16_t ms; const char* label; } pubOpts[] = {
    {    0, "No throttle — send every received packet"},
    { 1000, "Every 1 second"},
    { 3000, "Every 3 seconds"},
    { 5000, "Every 5 seconds"},
    {10000, "Every 10 seconds"},
    {30000, "Every 30 seconds"},
    {60000, "Every 1 minute"},
  };
  for (auto& o : pubOpts) {
    h += "<option value='" + String(o.ms) + "'" +
         String(config.publish_interval_ms == o.ms ? " selected" : "") +
         ">" + o.label + "</option>";
  }
  h += F("</select>"
         "<p class='hint'>Packets received during the gap are silently dropped "
         "and counted as <b>Throttled</b> on the status page.</p>");

  // --- BLE scan timing ---
  h += F("<hr><h3>BLE scan timing</h3>"
         "<div class='row'>"
         "<div><label>Scan interval (ms)</label>");
  h += "<input type='number' name='scan_iv_ms' id='siv' min='10' max='10240' value='"
       + String(config.ble_scan_interval_ms)
       + "' oninput=\"document.getElementById('swin').max=this.value\">";
  h += F("</div><div><label>Scan window (ms)</label>");
  h += "<input type='number' name='scan_win_ms' id='swin' min='10' max='"
       + String(config.ble_scan_interval_ms)
       + "' value='" + String(config.ble_scan_window_ms) + "'>";
  h += F("</div></div>"
         "<p class='hint'>"
         "<b>Window must be &le; interval.</b> "
         "Equal values (e.g. 63 / 62) = 100% duty cycle — most reliable, highest power. "
         "Wider gap = lower power but you may miss fast advertisements.<br>"
         "Presets &mdash; "
         "<a href='#' onclick=\"siv.value=63;swin.max=63;swin.value=62;return false\">Max (63/62)</a> &nbsp;"
         "<a href='#' onclick=\"siv.value=160;swin.max=160;swin.value=80;return false\">Balanced (160/80)</a> &nbsp;"
         "<a href='#' onclick=\"siv.value=1000;swin.max=1000;swin.value=100;return false\">Power save (1000/100)</a>"
         "</p>");

  // --- BLE filter ---
  h += F("<hr><h3>BLE MAC filter</h3>"
         "<label>Target MAC addresses (comma separated — blank = accept all)</label>");
  String macs;
  for (uint8_t i = 0; i < config.target_mac_count; i++) {
    if (i) macs += ", ";
    macs += config.target_macs[i];
  }
  h += "<input type='text' name='ble_mac' value='" + macs
       + "' placeholder='C300006C5E3F, AABBCCDDEEFF'>";
  h += F("<p class='hint'>Enter MACs exactly as shown in Serial monitor or nRF Connect. "
         "Colons are stripped automatically.</p>");

  h += F("<input type='submit' value='Save &amp; reboot'></form></div>");

  // --- Danger zone ---
  h += F("<div class='card'><h3>Danger zone</h3>"
         "<form action='/resetwifi' method='POST' onsubmit=\"return confirm('Erase ALL settings?')\">"
         "<button type='submit' class='danger'>Factory reset &amp; reboot</button>"
         "</form></div>");
  h += htmlFooter();
  return h;
}

// ---- Live diagnostics page ----
String getStatusHTML() {
  String h = htmlHeader("Diagnostics", "<meta http-equiv='refresh' content='5'>");
  h += F("<h2>Live diagnostics</h2><p class='sub'>Auto-refreshes every 5 s</p><div class='card'><div class='grid'>");

  bool wOk = WiFi.status() == WL_CONNECTED, mOk = mqttClient.connected();
  unsigned long up = (millis() - g_bootMs) / 1000;
  unsigned long sp = g_lastPublishMs > 0 ? (millis() - g_lastPublishMs) / 1000 : 0;

  auto badge = [](bool ok, const char* y, const char* n) {
    return String("<span class='badge ") + (ok?"ok'>":"bad'>") + (ok?y:n) + "</span>";
  };
  auto stat = [&](const char* l, String v) {
    h += "<div class='stat'><div class='lbl'>" + String(l) + "</div><div class='val'>" + v + "</div></div>";
  };

  stat("WiFi",           badge(wOk, "Connected", "Offline"));
  stat("WiFi RSSI",      String(wOk ? WiFi.RSSI() : 0) + " dBm");
  stat("MQTT",           badge(mOk, "Connected", "Offline"));
  stat("Uptime",         String(up/3600) + "h " + String((up%3600)/60) + "m");
  stat("Free heap",      String(esp_get_free_heap_size()/1024) + " KB");
  stat("Min free heap",  String(esp_get_minimum_free_heap_size()/1024) + " KB");
  stat("Queue",          String(uxQueueMessagesWaiting(sensorQueue)) + " / " + QUEUE_LENGTH);
  stat("Published",      String(g_publishedPackets));
  stat("Throttled",      String(g_throttledPackets));
  stat("Dropped (BLE)",  String(g_droppedPackets));
  stat("Last publish",   g_lastPublishMs > 0 ? String(sp) + "s ago" : String("never"));
  stat("Publish every",  config.publish_interval_ms == 0 ? String("immediately") : String(config.publish_interval_ms/1000) + "s / sensor");
  stat("Scan interval",  String(config.ble_scan_interval_ms) + " ms");
  stat("Scan window",    String(config.ble_scan_window_ms)   + " ms");

  h += F("</div></div><a href='/'>&larr; Back to configuration</a>");
  h += htmlFooter();
  return h;
}

// ============================================================
// REQUEST HANDLERS
// ============================================================
void handleAPRoot (AsyncWebServerRequest* r) { r->send(200, "text/html", getWifiSetupHTML()); }
void handleSTARoot(AsyncWebServerRequest* r) { r->send(200, "text/html", getMQTTSetupHTML()); }
void handleStatus (AsyncWebServerRequest* r) { r->send(200, "text/html", getStatusHTML()); }

void handleSaveWiFi(AsyncWebServerRequest* req) {
  if (!req->hasParam("ssid", true) || !req->hasParam("pass", true))
    { req->send(400, "text/plain", "Missing fields"); return; }
  preferences.begin("config", false);
  preferences.putString("ssid", req->getParam("ssid", true)->value());
  preferences.putString("pass", req->getParam("pass", true)->value());
  preferences.end();
  String h = htmlHeader("Saved");
  h += F("<div class='card' style='text-align:center'><h3>Credentials saved</h3>"
         "<p class='sub'>Rebooting to connect. If the password is wrong the gateway "
         "reopens its AP automatically.</p></div>");
  h += htmlFooter();
  req->send(200, "text/html", h);
  scheduleRestart(1500);
}

void handleSaveMQTT(AsyncWebServerRequest* req) {
  if (!req->hasParam("mqtt_server", true))
    { req->send(400, "text/plain", "Missing broker address"); return; }
  auto v = [&](const char* n) {
    return req->hasParam(n, true) ? req->getParam(n, true)->value() : String("");
  };
  preferences.begin("config", false);
  preferences.putString ("mqtt_server", v("mqtt_server"));
  preferences.putUShort ("mqtt_port",   v("mqtt_port").toInt());
  preferences.putString ("mqtt_user",   v("mqtt_user"));
  preferences.putString ("mqtt_pass",   v("mqtt_pass"));
  preferences.putString ("mqtt_topic",  v("mqtt_topic"));
  preferences.putUChar  ("mqtt_qos",    v("mqtt_qos").toInt());
  preferences.putBool   ("mqtt_retain", v("mqtt_retain") == "true");
  preferences.putString ("ble_mac",     v("ble_mac"));
  // BLE scan timing
  uint16_t iv  = max((uint16_t)10, (uint16_t)v("scan_iv_ms").toInt());
  uint16_t win = max((uint16_t)10, (uint16_t)v("scan_win_ms").toInt());
  if (win > iv) win = iv;
  preferences.putUShort("scan_iv_ms",  iv);
  preferences.putUShort("scan_win_ms", win);
  // Publish throttle
  preferences.putUShort("pub_iv_ms",   v("pub_iv_ms").toInt());
  preferences.end();
  String h = htmlHeader("Saved");
  h += F("<div class='card' style='text-align:center'><h3>Configuration saved</h3>"
         "<p class='sub'>Rebooting now&hellip;</p></div>");
  h += htmlFooter();
  req->send(200, "text/html", h);
  scheduleRestart(1500);
}

void handleResetWiFi(AsyncWebServerRequest* req) {
  preferences.begin("config", false);
  preferences.clear();
  preferences.end();
  String h = htmlHeader("Reset");
  h += F("<div class='card' style='text-align:center'><h3>Settings cleared</h3>"
         "<p class='sub'>Rebooting. Reconnect to <b>ESP32-Gateway-AP</b> to reconfigure.</p></div>");
  h += htmlFooter();
  req->send(200, "text/html", h);
  scheduleRestart(1500);
}

// ============================================================
// SETUP / LOOP
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(200);
  g_bootMs = millis();

  loadConfig();
  sensorQueue = xQueueCreate(QUEUE_LENGTH, sizeof(SensorPacket));

  if (strlen(g_ssid) == 0) {
    g_configMode = true;
    WiFi.mode(WIFI_AP_STA);
    WiFi.softAP("ESP32-Gateway-AP");
    // Scan networks here in setup() (main task, blocking-safe).
    // Results cached in g_ssidOptions; never re-scanned in an async handler.
    int n = WiFi.scanNetworks();
    for (int i = 0; i < n && i < 20; i++)
      g_ssidOptions += "<option value='" + WiFi.SSID(i) + "'>";
    WiFi.scanDelete();
    dnsServer.start(53, "*", WiFi.softAPIP()); // captive portal
    server.on("/", HTTP_GET, handleAPRoot);
    server.on("/savewifi", HTTP_POST, handleSaveWiFi);
    server.onNotFound([](AsyncWebServerRequest* r){ r->redirect("http://192.168.4.1/"); });
    server.begin();
    Serial.print("[Setup] AP mode — "); Serial.println(WiFi.softAPIP());
    return;
  }

  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(g_ssid, g_pass);
  int att = 0;
  while (WiFi.status() != WL_CONNECTED && att < 20) { delay(500); att++; }
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[Setup] Bad credentials — clearing and rebooting to AP mode.");
    preferences.begin("config", false); preferences.clear(); preferences.end();
    delay(500); esp_restart();
  }
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  Serial.print("[Setup] STA — "); Serial.println(WiFi.localIP());

  if (strlen(config.mqtt_server) > 0) {
    mqttClient.setServer(config.mqtt_server, config.mqtt_port);
    mqttClient.setBufferSize(512);
  }
  server.on("/",          HTTP_GET,  handleSTARoot);
  server.on("/savemqtt",  HTTP_POST, handleSaveMQTT);
  server.on("/resetwifi", HTTP_POST, handleResetWiFi);
  server.on("/status",    HTTP_GET,  handleStatus);
  server.begin();

  xTaskCreatePinnedToCore(bleTask, "BLE_Task", 4096, nullptr, 1, nullptr, 0);
  Serial.println("[Setup] Gateway ready.");
}

void loop() {
  if (g_configMode) {
    dnsServer.processNextRequest();
  } else {
    ensureWifiConnected();
    if (WiFi.status() == WL_CONNECTED) {
      ensureMqttConnected();
      mqttClient.loop();
      drainQueueToMqtt();
    }
    checkHeapHealth();
  }
  if (g_pendingRestart && millis() >= g_restartAtMs) esp_restart();
  delay(10);
}