#include <Arduino.h>
#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEScan.h>
#include <time.h>
#include <vector>

WebServer server(80);
Preferences preferences;
WiFiClient espClient;
PubSubClient mqttClient(espClient);
BLEScan* pBLEScan;

// --- GLOBAL VARIABLES ---
unsigned long lastScanTime = 0;
const int scanInterval = 10000; 
const int scanDuration = 5;     

std::vector<String> packetQueue;
portMUX_TYPE mux = portMUX_INITIALIZER_UNLOCKED;

// --- HELPER FUNCTIONS ---
String getIso8601Time() {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo, 10)) return "1970-01-01T00:00:00.000Z"; 
  char timeStringBuff[30];
  strftime(timeStringBuff, sizeof(timeStringBuff), "%Y-%m-%dT%H:%M:%S.000Z", &timeinfo);
  return String(timeStringBuff);
}

String cleanMacAddress(String mac) {
  mac.replace(":", "");
  mac.toUpperCase();
  return mac;
}

String bytesToHex(uint8_t* data, size_t length) {
  String hexStr = "";
  for (size_t i = 0; i < length; i++) {
    char buf[3];
    sprintf(buf, "%02X", data[i]);
    hexStr += buf;
  }
  return hexStr;
}

// --- DYNAMIC HTML GENERATORS ---
const char* htmlWiFiSetup = R"rawliteral(
<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{font-family: Arial; padding: 20px;} input{margin-bottom: 10px; padding: 5px; width: 100%; max-width: 300px;}</style></head><body>
<h2>ESP32 WiFi Setup</h2>
<form action="/savewifi" method="POST">
  SSID:<br><input type="text" name="ssid" required><br>
  Password:<br><input type="password" name="pass" required><br>
  <input type="submit" value="Connect">
</form>
</body></html>
)rawliteral";

// Dynamically builds the HTML so saved values persist in the form fields
String getMQTTSetupHTML() {
  String mqtt_server = preferences.getString("mqtt_server", "");
  String mqtt_port = preferences.getString("mqtt_port", "1883");
  String mqtt_user = preferences.getString("mqtt_user", "");
  String mqtt_pass = preferences.getString("mqtt_pass", "");
  String mqtt_topic = preferences.getString("mqtt_topic", "esp32/sensors/");
  String mqtt_qos = preferences.getString("mqtt_qos", "0");
  String mqtt_retain = preferences.getString("mqtt_retain", "false");
  String ble_mac = preferences.getString("ble_mac", "");

  String html = "<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">";
  html += "<style>body{font-family: Arial; padding: 20px;} input, select{margin-bottom: 15px; padding: 5px; width: 100%; max-width: 300px;}</style></head><body>";
  html += "<h2>ESP32 Gateway Configuration</h2>";
  html += "<form action=\"/savemqtt\" method=\"POST\">";
  
  html += "<b>MQTT Settings</b><br>";
  html += "Broker IP / Domain:<br><input type=\"text\" name=\"mqtt_server\" value=\"" + mqtt_server + "\" required><br>";
  html += "Broker Port:<br><input type=\"number\" name=\"mqtt_port\" value=\"" + mqtt_port + "\" required><br>";
  html += "Username (optional):<br><input type=\"text\" name=\"mqtt_user\" value=\"" + mqtt_user + "\"><br>";
  html += "Password (optional):<br><input type=\"password\" name=\"mqtt_pass\" value=\"" + mqtt_pass + "\"><br>";
  html += "Target Topic:<br><input type=\"text\" name=\"mqtt_topic\" value=\"" + mqtt_topic + "\" required><br>";
  
  html += "QoS:<br><select name=\"mqtt_qos\">";
  html += "<option value=\"0\"" + String(mqtt_qos == "0" ? " selected" : "") + ">0 - At most once</option>";
  html += "<option value=\"1\"" + String(mqtt_qos == "1" ? " selected" : "") + ">1 - At least once</option>";
  html += "</select><br>";
  
  html += "Retain Message:<br><select name=\"mqtt_retain\">";
  html += "<option value=\"false\"" + String(mqtt_retain == "false" ? " selected" : "") + ">False</option>";
  html += "<option value=\"true\"" + String(mqtt_retain == "true" ? " selected" : "") + ">True</option>";
  html += "</select><br><br>";
  
  html += "<b>BLE Filtering</b><br>";
  html += "Target MACs (comma separated):<br><input type=\"text\" name=\"ble_mac\" value=\"" + ble_mac + "\" placeholder=\"e.g. C300006C5E3F, AABBCCDDEEFF\"><br><br>";
  
  html += "<input type=\"submit\" value=\"Save Configuration\">";
  html += "</form><hr>";
  
  html += "<form action=\"/resetwifi\" method=\"POST\">";
  html += "<input type=\"submit\" value=\"Reset WiFi & Reboot\">";
  html += "</form></body></html>";

  return html;
}


// --- REAL-TIME BLE CALLBACK ---
class MyAdvertisedDeviceCallbacks : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice advertisedDevice) {
    String devMac = advertisedDevice.getAddress().toString().c_str();
    String cleanMac = cleanMacAddress(devMac);
    
    String targetMacs = preferences.getString("ble_mac", "");
    targetMacs.toUpperCase(); // Ensure case matches
    targetMacs.replace(":", ""); // Strip any colons just in case user types them

    // 1. Filter: If list is NOT empty, check if our current MAC is inside the list
    if (targetMacs != "" && targetMacs.indexOf(cleanMac) == -1) return;

    // 2. Only capture Service Data packets
    if (!advertisedDevice.haveServiceData()) return;

    // 3. Extract payload
    int rssi = advertisedDevice.getRSSI();
    String rawDataHex = bytesToHex(advertisedDevice.getPayload(), advertisedDevice.getPayloadLength());
    String timestamp = getIso8601Time();

    String jsonPayload = "{\n  \"timestamp\": \"" + timestamp + "\",\n  \"mac\": \"" + cleanMac + "\",\n  \"rssi\": " + String(rssi) + ",\n  \"rawData\": \"" + rawDataHex + "\"\n}";

    portENTER_CRITICAL(&mux);
    if (packetQueue.size() < 30) {
        packetQueue.push_back(jsonPayload);
    }
    portEXIT_CRITICAL(&mux);
  }
};


// --- WEB SERVER HANDLERS ---
void handleAPRoot() { server.send(200, "text/html", htmlWiFiSetup); }
void handleSTARoot() { server.send(200, "text/html", getMQTTSetupHTML()); } // Call the dynamic function here

void handleSaveWiFi() {
  if (server.hasArg("ssid") && server.hasArg("pass")) {
    WiFi.mode(WIFI_AP_STA);
    WiFi.begin(server.arg("ssid").c_str(), server.arg("pass").c_str());
    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 15) { delay(1000); attempts++; }

    if (WiFi.status() == WL_CONNECTED) {
      preferences.putString("ssid", server.arg("ssid"));
      preferences.putString("pass", server.arg("pass"));
      String newIP = WiFi.localIP().toString();
      server.send(200, "text/html", "<h2>Success!</h2><p>Click: <b><a href='http://" + newIP + "'>http://" + newIP + "</a></b></p><p>Rebooting...</p>");
      delay(3000); ESP.restart();
    } else {
      server.send(200, "text/html", "<h2>Failed</h2><button onclick='history.back()'>Go Back</button>");
      WiFi.disconnect(); WiFi.mode(WIFI_AP);
    }
  }
}

void handleSaveMQTT() {
  if (server.hasArg("mqtt_server")) {
    preferences.putString("mqtt_server", server.arg("mqtt_server"));
    preferences.putString("mqtt_port", server.arg("mqtt_port"));
    preferences.putString("mqtt_user", server.arg("mqtt_user"));
    preferences.putString("mqtt_pass", server.arg("mqtt_pass"));
    preferences.putString("mqtt_topic", server.arg("mqtt_topic"));
    preferences.putString("mqtt_qos", server.arg("mqtt_qos"));
    preferences.putString("mqtt_retain", server.arg("mqtt_retain"));
    preferences.putString("ble_mac", server.arg("ble_mac")); 
    
    server.send(200, "text/html", "<h2>Saved!</h2><p>Rebooting...</p>");
    delay(2000); ESP.restart(); 
  }
}

void handleResetWiFi() {
  preferences.clear();
  server.send(200, "text/html", "Cleared. Rebooting...");
  delay(2000); ESP.restart();
}


// --- SETUP ---
void setup() {
  Serial.begin(115200);
  preferences.begin("config", false);
  
  String ssid = preferences.getString("ssid", "");
  String pass = preferences.getString("pass", "");

  if (ssid == "") {
    WiFi.mode(WIFI_AP);
    WiFi.softAP("ESP32-Gateway-AP");
    server.on("/", HTTP_GET, handleAPRoot);
    server.on("/savewifi", HTTP_POST, handleSaveWiFi);
    server.begin();
  } else {
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid.c_str(), pass.c_str());

    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 20) { delay(1000); attempts++; }

    if (WiFi.status() == WL_CONNECTED) {
      configTime(0, 0, "pool.ntp.org");
      server.on("/", HTTP_GET, handleSTARoot);
      server.on("/savemqtt", HTTP_POST, handleSaveMQTT);
      server.on("/resetwifi", HTTP_POST, handleResetWiFi);
      server.begin();

      BLEDevice::init("");
      pBLEScan = BLEDevice::getScan(); 
      pBLEScan->setAdvertisedDeviceCallbacks(new MyAdvertisedDeviceCallbacks(), true);
      pBLEScan->setActiveScan(true);
      pBLEScan->setInterval(100);
      pBLEScan->setWindow(99); 
      
      String serverStr = preferences.getString("mqtt_server", "");
      if (serverStr != "") {
        mqttClient.setServer(serverStr.c_str(), preferences.getString("mqtt_port", "1883").toInt());
        mqttClient.setBufferSize(512); 
      }
    } else {
      preferences.clear(); delay(2000); ESP.restart();
    }
  }
}


// --- MAIN LOOP ---
void loop() {
  server.handleClient(); 
  
  if (WiFi.status() == WL_CONNECTED) {
    
    String serverStr = preferences.getString("mqtt_server", "");
    if (serverStr != "" && !mqttClient.connected()) {
       String clientId = "ESP32-Gateway-" + String(random(0xffff), HEX);
       String user = preferences.getString("mqtt_user", "");
       String pass = preferences.getString("mqtt_pass", "");
       if (user.length() > 0) mqttClient.connect(clientId.c_str(), user.c_str(), pass.c_str());
       else mqttClient.connect(clientId.c_str());
    }
    mqttClient.loop();

    if (millis() - lastScanTime > scanInterval) {
      pBLEScan->start(scanDuration, false); 
      pBLEScan->clearResults(); 
      lastScanTime = millis();
    }

    while (packetQueue.size() > 0 && mqttClient.connected()) {
      portENTER_CRITICAL(&mux);
      String payload = packetQueue.front();
      packetQueue.erase(packetQueue.begin());
      portEXIT_CRITICAL(&mux);

      String topic = preferences.getString("mqtt_topic", "esp32/sensors/");
      bool retain = preferences.getString("mqtt_retain", "false") == "true";
      
      mqttClient.publish(topic.c_str(), payload.c_str(), retain);
    }
  }
}