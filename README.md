# ESP32 BLE Project

This project implements a BLE application on an ESP32 using PlatformIO.

## Overview

The main program initializes the ESP32 BLE stack, configures a BLE server with services and characteristics, starts advertising, and handles BLE client connections and characteristic read/write events.

## Main Flow

1. Initialize serial output for debug logging.
2. Initialize the BLE device and set the BLE device name.
3. Create a BLE server instance and register callbacks for connection state changes.
4. Create a BLE service and add one or more BLE characteristics.
5. Configure characteristic properties and callbacks for read, write, and notification events.
6. Start the BLE service.
7. Start BLE advertising so clients can discover the ESP32.
8. In the main loop, handle BLE event processing, update characteristic values if needed, and optionally send notifications.

## Build and Upload

- Open the project in PlatformIO.
- Build the firmware.
- Upload to the ESP32.

## Notes

- Update service and characteristic UUIDs in `main.cpp` as needed.
- Ensure the ESP32 is powered and connected to the computer before uploading.
- Use a BLE client app to connect, read, write, and subscribe to notifications from the ESP32.
