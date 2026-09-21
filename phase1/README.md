# SmartNode Video Door Phone — Phase 1

## Goal

Phase 1 keeps camera access on the same LAN as the Raspberry Pi.

Flow:

Browser -> HTTP server -> Scan LAN
Browser -> MQTT start(device) -> Raspberry Pi
Raspberry Pi -> RTSP -> FFmpeg -> WebRTC -> Browser

## Files

- `public/index.html` — updated WebRTC frontend with Scan Network and device selection.
- `server.js` — serves the UI and `/api/scan`.
- `network-discovery.js` — ARP, optional ping sweep, TCP probes and ONVIF discovery.
- `webrtc-camera-server.js` — MQTT/WebRTC/FFmpeg server supporting per-session devices.
- `.env.example` — environment configuration.

## Install

On the Raspberry Pi:

```bash
cd ~/WebRTC-linux-code
npm install express mqtt wrtc
```

Copy these project files into the directory, or run this package from its own directory.

FFmpeg is required:

```bash
ffmpeg -version
```

## Configure

Create `.env` from `.env.example`:

```bash
cp .env.example .env
nano .env
```

Set MQTT credentials and the camera/NVR credentials.

Do not put camera or MQTT passwords into the frontend.

## Start

Terminal 1:

```bash
node server.js
```

Terminal 2:

```bash
node webrtc-camera-server.js
```

Open from a computer/phone on the same Wi-Fi/LAN:

```text
http://PI_IP:8080
```

For the current Pi:

```text
http://10.10.10.162:8080
```

## Scan

Click:

`Scan Network`

The frontend calls:

```text
POST /api/scan
```

The Raspberry Pi performs LAN discovery.

For a fuller scan, enable:

```bash
SMARTNODE_PING_SWEEP=1 node server.js
```

The ping sweep is concurrent but can still take longer than ARP/ONVIF-only discovery.

## Current RTSP support

Phase 1 includes the known Dahua-style template:

```text
/cam/realmonitor?channel=N&subtype=1
```

The actual username/password comes from environment variables.

Generic ONVIF discovery does not automatically reveal an authenticated RTSP URI. For other manufacturers, the next step is ONVIF authentication and `GetProfiles` / `GetStreamUri`, or a vendor-specific RTSP template.

## Multiple tabs

Each browser tab gets a unique WebRTC session ID.

The Pi stores:

```text
sessionId -> PeerConnection + RTCVideoSource + FFmpeg
```

Therefore separate tabs can have independent streams.

## Phase 2

Do not change this Phase 1 LAN architecture until it is stable.

Later, mobile-data access can add:

- Internet-safe frontend hosting
- MQTT/WSS authentication
- TURN
- WebRTC NAT traversal
- cloud/remote device routing

The camera/NVR itself should remain behind the customer's LAN.
