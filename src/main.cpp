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
#include <DNSServer.h>           // captive portal
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
#define MAX_TARGET_MACS         8
#define MAC_STR_LEN             13     // 12 hex chars + null
#define MAX_RAW_HEX_LEN         63     // 31 bytes payload -> 62 hex chars + null
#define QUEUE_LENGTH             40
#define BLE_SCAN_SECONDS          5
#define BLE_SCAN_REST_MS        200
#define WIFI_RECONNECT_MS     10000
#define MQTT_RECONNECT_MS      5000
#define HEAP_CHECK_MS          30000
#define HEAP_CRITICAL_BYTES    20000
#define PREVENTIVE_REBOOT_HOURS   0   // 0 = disabled

// ============================================================
// GLOBALS
// ============================================================
AsyncWebServer server(80);
DNSServer      dnsServer;          // only used in AP/config mode
Preferences    preferences;
WiFiClient     espClient;
PubSubClient   mqttClient(espClient);
NimBLEScan*    pBLEScan = nullptr;

// Cached at boot in setup() — NEVER rebuilt inside an async handler.
// WiFi.scanNetworks() inside an ESPAsyncWebServer callback (which runs
// on the AsyncTCP task, core 0) causes an abort() / CORRUPTED backtrace.
String g_ssidOptions = "";

struct Config {
  char     mqtt_server[64];
  uint16_t mqtt_port;
  char     mqtt_user[32];
  char     mqtt_pass[32];
  char     mqtt_topic[64];
  uint8_t  mqtt_qos;
  bool     mqtt_retain;
  char     target_macs[MAX_TARGET_MACS][MAC_STR_LEN];
  uint8_t  target_mac_count;
} config;

char g_ssid[33];
char g_pass[65];

struct SensorPacket {
  char  mac[MAC_STR_LEN];
  int8_t rssi;
  char  rawData[MAX_RAW_HEX_LEN];
  char  timestamp[26];
};

QueueHandle_t sensorQueue;
volatile uint32_t  g_droppedPackets   = 0;
volatile uint32_t  g_publishedPackets = 0;
volatile unsigned long g_lastPublishMs = 0;

unsigned long g_lastWifiAttempt = 0;
unsigned long g_lastMqttAttempt = 0;
unsigned long g_lastHeapCheck   = 0;
unsigned long g_bootMs          = 0;
bool          g_configMode      = false;

// Restart is scheduled, never called directly from an async handler,
// so the handler always returns before esp_restart() fires.
volatile bool          g_pendingRestart = false;
volatile unsigned long g_restartAtMs    = 0;

void scheduleRestart(unsigned long delayMs) {
  // Stop the BLE scan gracefully so core 0 isn't mid-operation when
  // esp_restart() fires — that was causing the CORRUPTED backtrace.
  if (pBLEScan != nullptr) pBLEScan->stop();
  g_pendingRestart = true;
  g_restartAtMs    = millis() + delayMs;
}

// ============================================================
// HELPERS — zero Arduino String, zero flash access in hot path
// ============================================================
void macBytesToStr(const uint8_t* addr, char* out) {
  // NimBLE stores address bytes LSB-first (addr[0] = least significant byte).
  // Human-readable MAC notation (what Minew app / nRF Connect shows) is
  // MSB-first, so we reverse the order here to keep filter comparison simple.
  snprintf(out, MAC_STR_LEN, "%02X%02X%02X%02X%02X%02X",
           addr[5], addr[4], addr[3], addr[2], addr[1], addr[0]);
}

void bytesToHexStr(const uint8_t* data, size_t len, char* out, size_t outSize) {
  size_t maxBytes = (outSize - 1) / 2;
  if (len > maxBytes) len = maxBytes;
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

void parseTargetMacs(const char* input) {
  config.target_mac_count = 0;
  char buf[160];
  strncpy(buf, input, sizeof(buf) - 1);
  buf[sizeof(buf) - 1] = '\0';
  char* token = strtok(buf, ",");
  while (token && config.target_mac_count < MAX_TARGET_MACS) {
    while (*token == ' ') token++;
    char clean[MAC_STR_LEN] = {0};
    uint8_t ci = 0;
    for (uint8_t i = 0; token[i] && ci < MAC_STR_LEN - 1; i++) {
      char c = toupper((unsigned char)token[i]);
      if (isxdigit((unsigned char)c)) clean[ci++] = c;
    }
    clean[ci] = '\0';
    if (ci == 12) strncpy(config.target_macs[config.target_mac_count++], clean, MAC_STR_LEN);
    token = strtok(nullptr, ",");
  }
}

void loadConfig() {
  preferences.begin("config", true);
  strncpy(g_ssid, preferences.getString("ssid", "").c_str(), sizeof(g_ssid) - 1);
  strncpy(g_pass, preferences.getString("pass", "").c_str(), sizeof(g_pass) - 1);
  strncpy(config.mqtt_server, preferences.getString("mqtt_server", "").c_str(), sizeof(config.mqtt_server) - 1);
  config.mqtt_port = preferences.getString("mqtt_port", "1883").toInt();
  strncpy(config.mqtt_user, preferences.getString("mqtt_user", "").c_str(), sizeof(config.mqtt_user) - 1);
  strncpy(config.mqtt_pass, preferences.getString("mqtt_pass", "").c_str(), sizeof(config.mqtt_pass) - 1);
  strncpy(config.mqtt_topic, preferences.getString("mqtt_topic", "esp32/sensors/").c_str(), sizeof(config.mqtt_topic) - 1);
  config.mqtt_qos    = preferences.getString("mqtt_qos", "0").toInt();
  config.mqtt_retain = preferences.getString("mqtt_retain", "false") == "true";
  parseTargetMacs(preferences.getString("ble_mac", "").c_str());
  preferences.end();
}

// ============================================================
// BLE SCANNING — NimBLE, core 0 task
// ============================================================
class MyScanCallbacks : public NimBLEAdvertisedDeviceCallbacks {
  void onResult(NimBLEAdvertisedDevice* dev) override {
    if (!dev->haveServiceData()) return;

    SensorPacket pkt;

    // getNative() → raw 6-byte address, zero allocation.
    // Fallback if your NimBLE version lacks getNative():
    //   std::string s = dev->getAddress().toString();
    //   uint8_t ci = 0;
    //   for (char c : s)
    //     if (isxdigit((unsigned char)c) && ci < MAC_STR_LEN-1)
    //       pkt.mac[ci++] = toupper(c);
    //   pkt.mac[ci] = '\0';
    macBytesToStr(dev->getAddress().getNative(), pkt.mac);

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
  pBLEScan->setInterval(100);
  pBLEScan->setWindow(99);
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
  char clientId[32];
  snprintf(clientId, sizeof(clientId), "ESP32-GW-%04X", (unsigned int)random(0xffff));
  bool ok = strlen(config.mqtt_user) > 0
    ? mqttClient.connect(clientId, config.mqtt_user, config.mqtt_pass)
    : mqttClient.connect(clientId);
  Serial.printf("[MQTT] Connect %s\n", ok ? "OK" : "FAILED");
}

void drainQueueToMqtt() {
  SensorPacket pkt;
  while (mqttClient.connected() && xQueueReceive(sensorQueue, &pkt, 0) == pdTRUE) {
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
  uint32_t freeHeap = esp_get_free_heap_size();
  Serial.printf("[Health] free=%u min=%u queue=%d pub=%u drop=%u\n",
    freeHeap, esp_get_minimum_free_heap_size(),
    uxQueueMessagesWaiting(sensorQueue), g_publishedPackets, g_droppedPackets);
  if (freeHeap < HEAP_CRITICAL_BYTES) {
    Serial.println("[Health] CRITICAL heap - rebooting.");
    scheduleRestart(200);
  }
#if PREVENTIVE_REBOOT_HOURS > 0
  if (now - g_bootMs > (unsigned long)PREVENTIVE_REBOOT_HOURS * 3600000UL) {
    Serial.println("[Health] Preventive reboot.");
    scheduleRestart(200);
  }
#endif
}

// ============================================================
// WEB UI TEMPLATES
// ============================================================
String htmlHeader(const char* title, const char* extraHead = "") {
  String h = F("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\">"
               "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,maximum-scale=1\">"
               "<title>");
  h += title;
  h += F("</title>");
  h += extraHead;
  h += F("<style>"
    ":root{--bg:#0f172a;--card:#1e293b;--text:#e2e8f0;--muted:#94a3b8;"
    "--accent:#38bdf8;--accent-dark:#0284c7;--ok:#4ade80;--bad:#f87171;--border:#334155}"
    "*{box-sizing:border-box}"
    "body{margin:0;padding:24px 16px;background:var(--bg);color:var(--text);"
    "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"
    "display:flex;justify-content:center}"
    ".container{width:100%;max-width:480px}"
    "h2{font-size:1.4rem;margin:0 0 4px}"
    "p.sub{color:var(--muted);margin:0 0 20px;font-size:.9rem}"
    ".card{background:var(--card);border:1px solid var(--border);border-radius:14px;"
    "padding:20px;margin-bottom:16px;box-shadow:0 4px 16px rgba(0,0,0,.25)}"
    ".card h3{margin:0 0 14px;font-size:1rem;color:var(--accent)}"
    "label{display:block;font-size:.85rem;color:var(--muted);margin:14px 0 6px}"
    "label:first-of-type{margin-top:0}"
    "input[type=text],input[type=password],input[type=number],select{"
    "width:100%;padding:12px 14px;border-radius:10px;border:1px solid var(--border);"
    "background:#0f172a;color:var(--text);font-size:1rem;outline:none;transition:border-color .15s}"
    "input:focus,select:focus{border-color:var(--accent)}"
    ".row{display:flex;gap:12px}.row>div{flex:1}"
    "button,input[type=submit]{width:100%;padding:13px;margin-top:18px;border:none;"
    "border-radius:10px;background:var(--accent);color:#04111f;font-weight:600;"
    "font-size:1rem;cursor:pointer;transition:background .15s}"
    "button:hover,input[type=submit]:hover{background:var(--accent-dark);color:#fff}"
    ".btn-danger{background:var(--bad);color:#1a0505}"
    ".btn-danger:hover{background:#dc2626;color:#fff}"
    ".grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}"
    ".stat{background:#0f172a;border:1px solid var(--border);border-radius:10px;padding:10px 12px}"
    ".stat .lbl{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.03em}"
    ".stat .val{font-size:1.05rem;font-weight:600;margin-top:2px}"
    ".badge{display:inline-block;padding:2px 9px;border-radius:999px;font-size:.75rem;font-weight:600}"
    ".ok{background:rgba(74,222,128,.15);color:var(--ok)}"
    ".bad{background:rgba(248,113,113,.15);color:var(--bad)}"
    "a{color:var(--accent);text-decoration:none;font-size:.85rem}"
    ".links{display:flex;justify-content:space-between;margin-top:10px}"
  "</style></head><body><div class=\"container\">");
  return h;
}

String htmlFooter() { return F("</div></body></html>"); }

// ---- AP / WiFi setup page ----
// Uses g_ssidOptions which was built with WiFi.scanNetworks() in setup().
// NEVER call WiFi.scanNetworks() here — this function runs inside an
// async request handler and that would cause an abort() on core 0.
String getWifiSetupHTML() {
  String h = htmlHeader("WiFi Setup");
  h += F("<h2>ESP32 Gateway</h2><p class=\"sub\">Connect to your WiFi network</p>"
         "<div class=\"card\"><form action=\"/savewifi\" method=\"POST\">"
         "<label>Network (SSID)</label>"
         "<input type=\"text\" name=\"ssid\" list=\"sl\" autocomplete=\"off\" required>"
         "<datalist id=\"sl\">");
  h += g_ssidOptions;   // pre-built in setup(), safe to read here
  h += F("</datalist>"
         "<label>Password</label>"
         "<input type=\"password\" name=\"pass\" required>"
         "<input type=\"submit\" value=\"Connect\">"
         "</form></div>");
  h += htmlFooter();
  return h;
}

// ---- STA / main config page ----
String getMQTTSetupHTML() {
  String h = htmlHeader("BLE Gateway Config");
  h += F("<h2>BLE &rarr; MQTT Gateway</h2><p class=\"sub\">Sensor gateway configuration</p>");

  bool wOk = WiFi.status() == WL_CONNECTED, mOk = mqttClient.connected();
  unsigned long up = (millis() - g_bootMs) / 1000;

  h += F("<div class=\"card\"><h3>Live status</h3><div class=\"grid\">");
  auto stat = [&](const char* lbl, String val) {
    h += "<div class=\"stat\"><div class=\"lbl\">" + String(lbl) + "</div><div class=\"val\">" + val + "</div></div>";
  };
  auto badge = [](bool ok, const char* yes, const char* no) -> String {
    return String("<span class=\"badge ") + (ok ? "ok\">" : "bad\">") + (ok ? yes : no) + "</span>";
  };

  stat("WiFi",       badge(wOk, "Connected", "Offline"));
  stat("MQTT",       badge(mOk, "Connected", "Offline"));
  stat("Uptime",     String(up / 3600) + "h " + String((up % 3600) / 60) + "m");
  stat("Free heap",  String(esp_get_free_heap_size() / 1024) + " KB");
  stat("Published",  String(g_publishedPackets));
  stat("Dropped",    String(g_droppedPackets));
  h += F("</div><div class=\"links\"><a href=\"/status\">Full diagnostics &rarr;</a><a href=\"/\">Refresh</a></div></div>");

  h += F("<div class=\"card\"><h3>MQTT broker</h3><form action=\"/savemqtt\" method=\"POST\">"
         "<label>Broker address</label>");
  h += "<input type=\"text\" name=\"mqtt_server\" value=\"" + String(config.mqtt_server) + "\" placeholder=\"192.168.1.10\" required>";
  h += F("<div class=\"row\"><div><label>Port</label>");
  h += "<input type=\"number\" name=\"mqtt_port\" value=\"" + String(config.mqtt_port) + "\" required></div>";
  h += F("<div><label>QoS</label><select name=\"mqtt_qos\">");
  h += "<option value=\"0\"" + String(config.mqtt_qos == 0 ? " selected" : "") + ">0 — At most once</option>";
  h += "<option value=\"1\"" + String(config.mqtt_qos == 1 ? " selected" : "") + ">1 — At least once</option>";
  h += F("</select></div></div>"
         "<label>Username (optional)</label>");
  h += "<input type=\"text\" name=\"mqtt_user\" value=\"" + String(config.mqtt_user) + "\">";
  h += F("<label>Password (optional)</label>");
  h += "<input type=\"password\" name=\"mqtt_pass\" value=\"" + String(config.mqtt_pass) + "\">";
  h += F("<label>Topic</label>");
  h += "<input type=\"text\" name=\"mqtt_topic\" value=\"" + String(config.mqtt_topic) + "\" required>";
  h += F("<label>Retain</label><select name=\"mqtt_retain\">");
  h += "<option value=\"false\"" + String(!config.mqtt_retain ? " selected" : "") + ">False</option>";
  h += "<option value=\"true\"" + String(config.mqtt_retain ? " selected" : "") + ">True</option>";
  h += F("</select>"
         "<h3 style=\"margin-top:22px\">BLE filtering</h3>"
         "<label>Target MACs (comma separated — blank = accept all)</label>");
  String macs = "";
  for (uint8_t i = 0; i < config.target_mac_count; i++) {
    if (i) macs += ", ";
    macs += config.target_macs[i];
  }
  h += "<input type=\"text\" name=\"ble_mac\" value=\"" + macs + "\" placeholder=\"C300006C5E3F, AABBCCDDEEFF\">";
  h += F("<input type=\"submit\" value=\"Save &amp; reboot\"></form></div>"
         "<div class=\"card\"><h3>Danger zone</h3>"
         "<form action=\"/resetwifi\" method=\"POST\" onsubmit=\"return confirm('Erase all settings?')\">"
         "<button type=\"submit\" class=\"btn-danger\">Factory reset &amp; reboot</button>"
         "</form></div>");
  h += htmlFooter();
  return h;
}

// ---- Diagnostics page ----
String getStatusHTML() {
  String h = htmlHeader("Diagnostics", "<meta http-equiv=\"refresh\" content=\"5\">");
  h += F("<h2>Live diagnostics</h2><p class=\"sub\">Auto-refreshes every 5 s</p>"
         "<div class=\"card\"><div class=\"grid\">");

  bool wOk = WiFi.status() == WL_CONNECTED, mOk = mqttClient.connected();
  unsigned long up = (millis() - g_bootMs) / 1000;
  unsigned long sinceP = g_lastPublishMs > 0 ? (millis() - g_lastPublishMs) / 1000 : 0;

  auto stat = [&](const char* lbl, String val) {
    h += "<div class=\"stat\"><div class=\"lbl\">" + String(lbl) + "</div><div class=\"val\">" + val + "</div></div>";
  };
  auto badge = [](bool ok, const char* y, const char* n) -> String {
    return String("<span class=\"badge ") + (ok?"ok\">":"bad\">") + (ok?y:n) + "</span>";
  };

  stat("WiFi",          badge(wOk, "Connected", "Offline"));
  stat("WiFi RSSI",     String(wOk ? WiFi.RSSI() : 0) + " dBm");
  stat("MQTT",          badge(mOk, "Connected", "Offline"));
  stat("Uptime",        String(up / 3600) + "h " + String((up % 3600) / 60) + "m");
  stat("Free heap",     String(esp_get_free_heap_size() / 1024) + " KB");
  stat("Min free heap", String(esp_get_minimum_free_heap_size() / 1024) + " KB");
  stat("Queue",         String(uxQueueMessagesWaiting(sensorQueue)) + " / " + QUEUE_LENGTH);
  stat("Published",     String(g_publishedPackets));
  stat("Dropped",       String(g_droppedPackets));
  stat("Last publish",  g_lastPublishMs > 0 ? String(sinceP) + "s ago" : String("never"));

  h += F("</div></div><a href=\"/\">&larr; Back to configuration</a>");
  h += htmlFooter();
  return h;
}

// ============================================================
// REQUEST HANDLERS
// ============================================================
void handleAPRoot(AsyncWebServerRequest* req)  { req->send(200, "text/html", getWifiSetupHTML()); }
void handleSTARoot(AsyncWebServerRequest* req) { req->send(200, "text/html", getMQTTSetupHTML()); }
void handleStatus(AsyncWebServerRequest* req)  { req->send(200, "text/html", getStatusHTML()); }

// Previously this called WiFi.begin() + delay() inside the async handler —
// blocking the AsyncTCP task for up to 15 s, causing instability.
// Now we just save credentials and reboot; if the password is wrong the
// next boot falls back to AP mode automatically.
void handleSaveWiFi(AsyncWebServerRequest* req) {
  if (!req->hasParam("ssid", true) || !req->hasParam("pass", true)) {
    req->send(400, "text/plain", "Missing fields"); return;
  }
  preferences.begin("config", false);
  preferences.putString("ssid", req->getParam("ssid", true)->value());
  preferences.putString("pass", req->getParam("pass", true)->value());
  preferences.end();

  String h = htmlHeader("Connecting");
  h += "<div class=\"card\" style=\"text-align:center\"><h3>Credentials saved</h3>"
       "<p class=\"sub\">Rebooting to connect. If the password is wrong the gateway "
       "will reopen its AP so you can try again.</p></div>";
  h += htmlFooter();
  req->send(200, "text/html", h);
  scheduleRestart(1500);
}

void handleSaveMQTT(AsyncWebServerRequest* req) {
  if (!req->hasParam("mqtt_server", true)) {
    req->send(400, "text/plain", "Missing fields"); return;
  }
  auto v = [&](const char* n) -> String {
    return req->hasParam(n, true) ? req->getParam(n, true)->value() : "";
  };
  preferences.begin("config", false);
  preferences.putString("mqtt_server", v("mqtt_server"));
  preferences.putString("mqtt_port",   v("mqtt_port"));
  preferences.putString("mqtt_user",   v("mqtt_user"));
  preferences.putString("mqtt_pass",   v("mqtt_pass"));
  preferences.putString("mqtt_topic",  v("mqtt_topic"));
  preferences.putString("mqtt_qos",    v("mqtt_qos"));
  preferences.putString("mqtt_retain", v("mqtt_retain"));
  preferences.putString("ble_mac",     v("ble_mac"));
  preferences.end();

  String h = htmlHeader("Saved");
  h += "<div class=\"card\" style=\"text-align:center\"><h3>Configuration saved</h3>"
       "<p class=\"sub\">Rebooting now&hellip;</p></div>";
  h += htmlFooter();
  req->send(200, "text/html", h);
  scheduleRestart(1500);
}

void handleResetWiFi(AsyncWebServerRequest* req) {
  preferences.begin("config", false);
  preferences.clear();
  preferences.end();

  String h = htmlHeader("Reset");
  h += "<div class=\"card\" style=\"text-align:center\"><h3>Settings cleared</h3>"
       "<p class=\"sub\">Rebooting. Reconnect to <b>ESP32-Gateway-AP</b> to reconfigure.</p></div>";
  h += htmlFooter();
  req->send(200, "text/html", h);
  scheduleRestart(1500);
}

// ============================================================
// SETUP
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(200);
  g_bootMs = millis();

  loadConfig();
  sensorQueue = xQueueCreate(QUEUE_LENGTH, sizeof(SensorPacket));

  // ---- AP / config mode ----
  if (strlen(g_ssid) == 0) {
    g_configMode = true;
    WiFi.mode(WIFI_AP_STA);   // STA side used only for the network scan below
    WiFi.softAP("ESP32-Gateway-AP");

    // Scan here in setup() (main task, core 1) — safe to call blocking scan.
    // Results cached in g_ssidOptions and reused by every page request
    // without ever touching the radio from inside an async handler.
    int n = WiFi.scanNetworks();
    for (int i = 0; i < n && i < 20; i++)
      g_ssidOptions += "<option value=\"" + WiFi.SSID(i) + "\">";
    WiFi.scanDelete();

    // Captive portal: any DNS query gets the AP IP so phones/laptops
    // open the config page automatically when joining the AP.
    dnsServer.start(53, "*", WiFi.softAPIP());

    server.on("/", HTTP_GET, handleAPRoot);
    server.on("/savewifi", HTTP_POST, handleSaveWiFi);
    // Redirect every unknown URL to the setup page (captive portal UX)
    server.onNotFound([](AsyncWebServerRequest* req){
      req->redirect("http://192.168.4.1/");
    });
    server.begin();
    Serial.print("[Setup] AP mode — gateway IP: ");
    Serial.println(WiFi.softAPIP());
    return;
  }

  // ---- STA / normal mode ----
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(g_ssid, g_pass);

  int att = 0;
  while (WiFi.status() != WL_CONNECTED && att < 20) { delay(500); att++; }

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[Setup] Bad saved credentials — clearing and falling back to AP mode");
    preferences.begin("config", false);
    preferences.clear();
    preferences.end();
    delay(500);
    esp_restart();
  }

  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  Serial.print("[Setup] STA connected — IP: ");
  Serial.println(WiFi.localIP());

  if (strlen(config.mqtt_server) > 0) {
    mqttClient.setServer(config.mqtt_server, config.mqtt_port);
    mqttClient.setBufferSize(512);
  }

  server.on("/", HTTP_GET, handleSTARoot);
  server.on("/savemqtt", HTTP_POST, handleSaveMQTT);
  server.on("/resetwifi", HTTP_POST, handleResetWiFi);
  server.on("/status", HTTP_GET, handleStatus);
  server.begin();

  xTaskCreatePinnedToCore(bleTask, "BLE_Task", 4096, nullptr, 1, nullptr, 0);
  Serial.println("[Setup] Gateway ready");
}

// ============================================================
// LOOP
// ============================================================
void loop() {
  if (g_configMode) {
    dnsServer.processNextRequest(); // captive portal DNS
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