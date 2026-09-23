# WebRTC Internet Streaming Debugging Guide

## Current Setup Issues (Without TURN)

Your setup is:
- **LAN**: Works ✓ (using RTSP → MJPEG)
- **Internet**: Not working ✗ (using MQTT + WebRTC)

## Why Internet Streaming Fails Without TURN

### Network Topology Requirements
For WebRTC to work over the internet WITHOUT TURN servers, you need:

1. **Raspberry Pi MUST have**:
   - Public IP address, OR
   - Port forwarding configured on your router
   - UDP ports 10000-60000 open (for RTP media)

2. **Browser Client**:
   - Can usually work behind NAT with STUN
   - STUN helps discover public IP

### Most Common Issue: Symmetric NAT
If your Raspberry Pi is behind a **Symmetric NAT**, WebRTC will fail without TURN because:
- Server creates ICE candidates with local IP (192.168.x.x)
- STUN provides public IP, but NAT changes ports for each destination
- Browser cannot reach the Pi's media stream

## Debugging Steps

### Step 1: Check ICE Candidates (CRITICAL)

**On Browser Console**, look for:
```
[ICE] Candidate: candidate:... typ srflx ...
```

**On Pi Server Logs**, look for:
```
[ICE OFFER CANDIDATES] session=...
a=candidate:... typ srflx ...
```

**What to check**:
- `typ host` = local network address (won't work over internet)
- `typ srflx` = server reflexive (public IP from STUN) ← YOU NEED THIS
- `typ relay` = TURN relay (you're not using this)

### Step 2: Verify Both Sides Have Public Candidates

**Expected in browser**:
```
[ICE PAIR] LOCAL=srflx <PUBLIC_IP>:<PORT> REMOTE=srflx <PUBLIC_IP>:<PORT>
```

**If you see**:
```
[ICE PAIR] LOCAL=host 192.168.x.x REMOTE=host 192.168.x.x
```
↑ This means no public IPs = Internet connection WILL FAIL

### Step 3: Check Raspberry Pi Network

Run on Pi:
```bash
# Check if Pi has public IP
curl ifconfig.me

# Check if behind NAT
ip addr show

# Test STUN from Pi
npm install -g stun
stun stun.l.google.com
```

### Step 4: Check ICE Connection State

**Browser console should show**:
```
[ICE] state=checking  ← Testing candidates
[ICE] state=connected ← SUCCESS!
```

**If stuck at**:
```
[ICE] state=checking (forever)
[ICE] state=failed
```
↑ Means ICE candidates cannot reach each other

## Solutions

### Option 1: Use TURN Server (Recommended for Internet)
Without TURN, you're limited by network topology.

### Option 2: Configure Port Forwarding
If Pi is behind NAT:
1. Forward UDP ports 10000-60000 to Pi's local IP
2. This is complex and not recommended

### Option 3: Deploy Pi with Public IP
- Use cloud VPS
- Use public IPv4 address

### Option 4: Stay with LAN Only
Your current LAN mode works perfectly - consider this for local deployment.

## Quick Test

To verify if your network supports WebRTC without TURN:

**On Pi, run this to see available candidates**:
```bash
# Check Pi's public IP
curl https://api.ipify.org

# Check if Pi can bind to UDP
netstat -ln | grep udp
```

**Check browser console for**:
```
[ICE PAIR] state=failed
```

If state stays at "failed", you **NEED** TURN servers or network reconfiguration.
