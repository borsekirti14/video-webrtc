/**
 * ================================================================
 * SmartNode Camera Server
 * LAN RTSP + Internet WebRTC
 * ================================================================
 *
 * LAN:
 *
 * Browser
 *    |
 *    | HTTP MJPEG :8091
 *    v
 * Raspberry Pi
 *    |
 *    | RTSP
 *    v
 * Camera / NVR
 *
 *
 * INTERNET:
 *
 * Browser
 *    |
 *    | MQTT signaling
 *    v
 * MQTT Broker
 *    |
 *    v
 * Raspberry Pi
 *    |
 *    | RTSP
 *    v
 * Camera / NVR
 *    |
 *    | WebRTC
 *    v
 * Browser
 *
 *
 * Ports:
 *
 * 8090 = internal camera verification
 *        localhost ONLY
 *
 * 8091 = LAN MJPEG streaming
 *        LAN/private clients
 *
 * MQTT = Internet WebRTC signaling
 *
 * ================================================================
 */

require("dotenv").config();

const http = require("http");
const mqtt = require("mqtt");
const wrtc = require("wrtc");
const { spawn } = require("child_process");

const {
    RTCPeerConnection,
    RTCSessionDescription
} = wrtc;

const {
    RTCVideoSource
} = wrtc.nonstandard;

const {
    getCamera,
    listCameras
} = require("./camera-config");

/* ================================================================
 * CONFIGURATION
 * ================================================================ */

const MQTT_URL =
    process.env.MQTT_URL ||
    "tcp://connect.smartnode.in:1883";

const MQTT_USERNAME =
    process.env.MQTT_USERNAME || "smartnode.kirti@gmail.com";

const MQTT_PASSWORD =
    process.env.MQTT_PASSWORD || "1234";

/* ------------------------------------------------
 * MQTT topics
 * ------------------------------------------------ */

const REQUEST_TOPIC =
    "smartnode/test/webrtc/request";

const OFFER_TOPIC_PREFIX =
    "smartnode/test/webrtc/offer";

const ANSWER_TOPIC_PREFIX =
    "smartnode/test/webrtc/answer";

const CAMERA_LIST_REQUEST_TOPIC =
    "smartnode/test/webrtc/cameras/request";

const CAMERA_LIST_RESPONSE_PREFIX =
    "smartnode/test/webrtc/cameras/response";

/* ------------------------------------------------
 * Internal HTTP server
 * ------------------------------------------------ */

const INTERNAL_HOST =
    process.env.WEBRTC_INTERNAL_HOST ||
    "127.0.0.1";

const INTERNAL_PORT =
    Number(
        process.env.WEBRTC_INTERNAL_PORT ||
        8090
    );

/* ------------------------------------------------
 * LAN HTTP streaming server
 * ------------------------------------------------ */

const LAN_STREAM_HOST =
    process.env.LAN_STREAM_HOST ||
    "0.0.0.0";

const LAN_STREAM_PORT =
    Number(
        process.env.LAN_STREAM_PORT ||
        8091
    );

/* ------------------------------------------------
 * WebRTC video
 * ------------------------------------------------ */

const WIDTH = 640;
const HEIGHT = 360;
const FPS = 25;

/*
 * I420 frame size.
 *
 * Y = width * height
 * U = width/2 * height/2
 * V = width/2 * height/2
 */
const FRAME_SIZE =
    WIDTH * HEIGHT +
    (WIDTH / 2) * (HEIGHT / 2) +
    (WIDTH / 2) * (HEIGHT / 2);

/* ------------------------------------------------
 * WebRTC ICE
 * ------------------------------------------------ */

const ICE_SERVERS = [
  {
    urls: [
      "stun:stun.l.google.com:19302",
      "stun:stun1.l.google.com:19302",
      "stun:stun2.l.google.com:19302",
      "stun:stun3.l.google.com:19302",
      "stun:stun4.l.google.com:19302"
    ]
  },
  // FREE TURN relay for testing - this will allow internet streaming
  // Replace with your own TURN server for production
  {
    urls: "turn:openrelay.metered.ca:80",
    username: "openrelayproject",
    credential: "openrelayproject"
  },
  {
    urls: "turn:openrelay.metered.ca:443",
    username: "openrelayproject",
    credential: "openrelayproject"
  },
  {
    urls: "turn:openrelay.metered.ca:443?transport=tcp",
    username: "openrelayproject",
    credential: "openrelayproject"
  }
];

/* ================================================================
 * GLOBAL STATE
 * ================================================================ */

/*
 * Internet WebRTC sessions.
 *
 * sessionId -> session
 */
const sessions = new Map();

/*
 * LAN RTSP/MJPEG streams.
 *
 * streamKey -> stream
 */
const lanStreams = new Map();

/*
 * MQTT client is declared before helper functions.
 */
let mqttClient = null;

/* ================================================================
 * GENERAL HELPERS
 * ================================================================ */

function safeString(value) {

    return (
        value === undefined ||
        value === null
    )
        ? ""
        : String(value);
}

function topicFor(
    prefix,
    sessionId
) {

    return `${prefix}/${sessionId}`;
}

function redactRtspUrl(url) {

    return String(url || "")
        .replace(
            /:\/\/([^:@/]+):([^@/]+)@/g,
            "://***:***@"
        );
}

/* ================================================================
 * IP HELPERS
 * ================================================================ */

function normalizeRemoteAddress(
    address
) {

    if (!address) {
        return "";
    }

    let value =
        String(address);

    /*
     * IPv4-mapped IPv6:
     *
     * ::ffff:192.168.1.20
     */
    if (
        value.startsWith(
            "::ffff:"
        )
    ) {

        value =
            value.substring(7);
    }

    if (
        value === "::1"
    ) {

        return "127.0.0.1";
    }

    return value;
}

function isPrivateIPv4(
    ip
) {

    const parts =
        String(ip)
            .split(".")
            .map(Number);

    if (
        parts.length !== 4 ||
        parts.some(
            value =>
                !Number.isInteger(value) ||
                value < 0 ||
                value > 255
        )
    ) {

        return false;
    }

    const [
        a,
        b
    ] = parts;

    /*
     * 10.0.0.0/8
     */
    if (
        a === 10
    ) {

        return true;
    }

    /*
     * 172.16.0.0/12
     */
    if (
        a === 172 &&
        b >= 16 &&
        b <= 31
    ) {

        return true;
    }

    /*
     * 192.168.0.0/16
     */
    if (
        a === 192 &&
        b === 168
    ) {

        return true;
    }

    /*
     * localhost
     */
    if (
        a === 127
    ) {

        return true;
    }

    /*
     * link local
     */
    if (
        a === 169 &&
        b === 254
    ) {

        return true;
    }

    return false;
}

function isAllowedLanClient(
    req
) {

    const remoteAddress =
        normalizeRemoteAddress(
            req.socket.remoteAddress
        );

    return isPrivateIPv4(
        remoteAddress
    );
}

/* ================================================================
 * RTSP URL BUILDER
 * ================================================================ */

function buildRtspUrl(
    device
) {

    if (
        !device ||
        !device.ip
    ) {

        throw new Error(
            "Camera IP is missing"
        );
    }

    /*
     * Credentials must come from the encrypted
     * camera configuration.
     */
    const username =
        device.credentials?.username;

    const password =
        device.credentials?.password;

    if (
        !username ||
        !password
    ) {

        throw new Error(
            `Stored camera credentials are missing for ${device.ip}`
        );
    }

    const user =
        encodeURIComponent(
            String(username)
        );

    const pass =
        encodeURIComponent(
            String(password)
        );

    /*
     * Custom RTSP URL.
     */
    if (
        device.rtspUrl
    ) {

        return String(
            device.rtspUrl
        )
            .replace(
                "{username}",
                user
            )
            .replace(
                "{password}",
                pass
            );
    }

    const type =
        String(
            device.type ||
            device.manufacturer ||
            ""
        ).toLowerCase();

    const channel =
        Number(
            device.channel || 1
        );

    const subtype =
        Number(
            device.subtype ?? 0
        );

    const port =
        Number(
            device.rtspPort || 554
        );

    /*
     * Dahua / NVR.
     *
     * Your discovered Dahua/NVR devices may appear
     * as type=camera, so rtsp=true and port 37777
     * are also checked.
     */
    if (
        type.includes("dahua") ||
        type.includes("nvr") ||
        device.ports?.includes?.(37777) ||
        device.rtsp === true
    ) {

        return (
            `rtsp://${user}:${pass}` +
            `@${device.ip}:${port}` +
            `/cam/realmonitor` +
            `?channel=${channel}` +
            `&subtype=${subtype}`
        );
    }

    /*
     * Generic RTSP path.
     */
    if (
        device.rtspPath
    ) {

        const path =
            String(
                device.rtspPath
            )
                .replace(
                    "{channel}",
                    String(channel)
                )
                .replace(
                    "{subtype}",
                    String(subtype)
                );

        return (
            `rtsp://${user}:${pass}` +
            `@${device.ip}:${port}` +
            path
        );
    }

    throw new Error(
        `No RTSP configuration found for ${device.ip}`
    );
}

/* ================================================================
 * MQTT PUBLISH
 * ================================================================ */

function publishJson(
    topic,
    payload
) {

    if (
        !mqttClient ||
        !mqttClient.connected
    ) {

        console.warn(
            "[MQTT] Cannot publish - MQTT not connected"
        );

        return;
    }

    const message =
        JSON.stringify(
            payload
        );

    mqttClient.publish(
        topic,
        message,
        {
            qos: 0,
            retain: false
        },
        error => {

            if (error) {

                console.error(
                    "[MQTT] Publish error:",
                    error.message
                );
            }
        }
    );
}

/* ================================================================
 * ICE GATHERING
 * ================================================================ */

function waitForIceComplete(
    pc,
    timeoutMs = 15000
) {

    if (
        pc.iceGatheringState ===
        "complete"
    ) {

        return Promise.resolve();
    }

    return new Promise(
        resolve => {

            let finished =
                false;

            let timer;

            function cleanup() {

                clearTimeout(
                    timer
                );

                pc.removeEventListener(
                    "icegatheringstatechange",
                    check
                );
            }

            function finish() {

                if (
                    finished
                ) {

                    return;
                }

                finished =
                    true;

                cleanup();

                resolve();
            }

            function check() {

                if (
                    pc.iceGatheringState ===
                    "complete"
                ) {

                    finish();
                }
            }

            timer =
                setTimeout(
                    finish,
                    timeoutMs
                );

            pc.addEventListener(
                "icegatheringstatechange",
                check
            );

            check();
        }
    );
}

/* ================================================================
 * WEBRTC SESSION CLEANUP
 * ================================================================ */

function stopSession(
    sessionId,
    reason = "stopped"
) {

    const session =
        sessions.get(
            sessionId
        );

    if (!session) {
        return;
    }

    session.stopping =
        true;

    console.log(
        `[SESSION ${sessionId}] stopping: ${reason}`
    );

    if (
        session.ffmpeg
    ) {

        try {

            session.ffmpeg.kill(
                "SIGKILL"
            );

        } catch (error) {}
    }

    if (
        session.videoTrack
    ) {

        try {

            session.videoTrack.stop();

        } catch (error) {}
    }

    if (
        session.pc
    ) {

        try {

            session.pc.close();

        } catch (error) {}
    }

    sessions.delete(
        sessionId
    );
}

/* ================================================================
 * WEBRTC FFMPEG
 * ================================================================ */

function startWebRtcFfmpeg(
    sessionId,
    rtspUrl,
    videoSource
) {

    console.log(
        `[SESSION ${sessionId}] RTSP:`,
        redactRtspUrl(
            rtspUrl
        )
    );

    const args = [

        "-hide_banner",

        "-loglevel",
        "warning",

        "-rtsp_transport",
        "tcp",

        "-timeout",
        "5000000",

        "-i",
        rtspUrl,

        "-an",

        "-vf",
        `scale=${WIDTH}:${HEIGHT}:force_original_aspect_ratio=decrease,pad=${WIDTH}:${HEIGHT}:(ow-iw)/2:(oh-ih)/2`,

        "-r",
        String(FPS),

        "-pix_fmt",
        "yuv420p",

        "-f",
        "rawvideo",

        "pipe:1"
    ];

    console.log(
        `[SESSION ${sessionId}] Starting FFmpeg with args:`,
        args.join(" ")
    );

    const ffmpeg =
        spawn(
            "ffmpeg",
            args,
            {
                stdio: [
                    "ignore",
                    "pipe",
                    "pipe"
                ]
            }
        );

    let frameBuffer =
        Buffer.alloc(0);

    let frameCount = 0;

    ffmpeg.stdout.on(
        "data",
        chunk => {

            /*
             * FFmpeg stdout chunks are not guaranteed
             * to contain complete frames.
             */
            frameBuffer =
                Buffer.concat([
                    frameBuffer,
                    chunk
                ]);

            while (
                frameBuffer.length >=
                FRAME_SIZE
            ) {

                const frame =
                    frameBuffer.subarray(
                        0,
                        FRAME_SIZE
                    );

                frameBuffer =
                    frameBuffer.subarray(
                        FRAME_SIZE
                    );

                frameCount++;

                try {

                    /*
                     * wrtc@0.4.7 expects an ArrayBuffer.
                     */
                    const frameArrayBuffer =
                        frame.buffer.slice(
                            frame.byteOffset,
                            frame.byteOffset +
                                frame.byteLength
                        );

                    videoSource.onFrame({
                        width:
                            WIDTH,

                        height:
                            HEIGHT,

                        data:
                            frameArrayBuffer
                    });

                    if (
                        frameCount % 100 ===
                        0
                    ) {

                        console.log(
                            `[SESSION ${sessionId}] frames sent: ${frameCount}`
                        );
                    }

                } catch (error) {

                    console.error(
                        `[SESSION ${sessionId}] frame error:`,
                        error.message
                    );
                }
            }
        }
    );

    ffmpeg.stderr.on(
        "data",
        data => {

            const text =
                data
                    .toString()
                    .trim();

            if (
                text
            ) {

                console.log(
                    `[FFMPEG ${sessionId}] ${text}`
                );
            }
        }
    );

    ffmpeg.on(
        "error",
        error => {

            console.error(
                `[SESSION ${sessionId}] FFmpeg spawn error:`,
                error.message
            );

            console.error(
                `[SESSION ${sessionId}] This usually means:`,
                `\n  1. FFmpeg is not installed`,
                `\n  2. FFmpeg binary not in PATH`,
                `\n  3. Permission denied`
            );
        }
    );

    ffmpeg.on(
        "exit",
        (
            code,
            signal
        ) => {

            console.log(
                `[SESSION ${sessionId}] FFmpeg exited code=${code} signal=${signal}`
            );

            if (code !== 0 && code !== 143 && code !== null) {
                console.error(
                    `[SESSION ${sessionId}] FFmpeg failed with code ${code}. Common reasons:`,
                    `\n  1. Camera unreachable (check: ping ${rtspUrl.match(/@([^:/@]+)/)?.[1] || 'camera-ip'})`,
                    `\n  2. Wrong RTSP credentials`,
                    `\n  3. Camera RTSP port closed/firewall`,
                    `\n  4. Network routing issue`,
                    `\n  5. Camera doesn't support this RTSP path`
                );
            }

            const session =
                sessions.get(
                    sessionId
                );

            if (
                session &&
                !session.stopping
            ) {

                stopSession(
                    sessionId,
                    "ffmpeg-exited"
                );
            }
        }
    );

    return ffmpeg;
}

/* ================================================================
 * CREATE WEBRTC SESSION
 * ================================================================ */

async function logServerIcePairs(session) {
  try {
    const stats = await session.pc.getStats();

    const candidates = new Map();

    for (const report of stats.values()) {
      if (
        report.type === "local-candidate" ||
        report.type === "remote-candidate"
      ) {
        candidates.set(report.id, report);
      }
    }

    console.log(
      `[ICE PAIRS] ===== session=${session.sessionId} =====`
    );

    let foundPairs = false;

    for (const report of stats.values()) {
      if (report.type !== "candidate-pair") {
        continue;
      }

      foundPairs = true;

      const local = candidates.get(report.localCandidateId);
      const remote = candidates.get(report.remoteCandidateId);

      console.log(
        `[ICE PAIR] ` +
        `state=${report.state} ` +
        `nominated=${report.nominated} ` +
        `selected=${report.selected} | ` +
        `LOCAL=${local?.candidateType} ` +
        `${local?.address}:${local?.port} ` +
        `protocol=${local?.protocol} | ` +
        `REMOTE=${remote?.candidateType} ` +
        `${remote?.address}:${remote?.port} ` +
        `protocol=${remote?.protocol} | ` +
        `sent=${report.bytesSent ?? 0} ` +
        `received=${report.bytesReceived ?? 0} ` +
        `rtt=${report.currentRoundTripTime ?? "n/a"}`
      );
    }

    if (!foundPairs) {
      console.error(
        `[ICE PAIRS] NO CANDIDATE PAIRS FOUND!`
      );
      console.error(
        `[ICE PAIRS] This means ICE negotiation hasn't started yet or completely failed.`
      );
      
      // Log what candidates we do have
      console.log(`[ICE PAIRS] Local candidates found:`);
      for (const report of stats.values()) {
        if (report.type === "local-candidate") {
          console.log(
            `  - ${report.candidateType} ${report.address}:${report.port} ${report.protocol}`
          );
        }
      }
      
      console.log(`[ICE PAIRS] Remote candidates found:`);
      for (const report of stats.values()) {
        if (report.type === "remote-candidate") {
          console.log(
            `  - ${report.candidateType} ${report.address}:${report.port} ${report.protocol}`
          );
        }
      }
    }

    console.log(
      `[ICE PAIRS] ================================`
    );

  } catch (error) {
    console.error(
      `[ICE PAIRS] error: ${error.message}`
    );
  }
}

async function createWebRtcSession(
    sessionId,
    camera
) {

    if (
        sessions.has(
            sessionId
        )
    ) {

        stopSession(
            sessionId,
            "duplicate-session"
        );
    }

    const rtspUrl =
        buildRtspUrl(
            camera
        );

    console.log(
        `[SESSION ${sessionId}] starting camera ${camera.ip}`
    );

    console.log(
        `[SESSION ${sessionId}] Creating RTCPeerConnection with ICE servers:`,
        JSON.stringify(ICE_SERVERS)
    );

    const pc =
        new RTCPeerConnection({
            iceServers:
                ICE_SERVERS,

            iceCandidatePoolSize:
                10
        });

    console.log(
        `[SESSION ${sessionId}] PeerConnection created, initial state:`,
        `connection=${pc.connectionState}`,
        `ice=${pc.iceConnectionState}`,
        `signaling=${pc.signalingState}`
    );

    const videoSource =
        new RTCVideoSource();

    const videoTrack =
        videoSource.createTrack();

    pc.addTransceiver(
        videoTrack,
        {
            direction:
                "sendonly"
        }
    );

    const session = {

        sessionId,

        cameraId:
            camera.deviceId,

        pc,

        videoSource,

        videoTrack,

        ffmpeg:
            null,

        stopping:
            false,

        createdAt:
            Date.now()
    };

    sessions.set(
        sessionId,
        session
    );

    /*
     * ICE gathering diagnostics.
     */
    pc.onicegatheringstatechange =
        () => {

            console.log(
                `[SESSION ${sessionId}] ICE gathering:`,
                pc.iceGatheringState
            );
        };

    /*
     * ICE connection diagnostics.
     */
    pc.oniceconnectionstatechange =
        async () => {

            console.log(
                `[SESSION ${sessionId}] ICE connection:`,
                pc.iceConnectionState
            );

            if (
                session.pc.iceConnectionState === "connected" ||
                session.pc.iceConnectionState === "completed" ||
                session.pc.iceConnectionState === "disconnected" ||
                session.pc.iceConnectionState === "failed"
            ) {
                await logServerIcePairs(session);
            }

            // Only stop on failed, not on "new" or "closed" during setup
            if (
                pc.iceConnectionState === "failed"
            ) {

                stopSession(
                    sessionId,
                    `ice-${pc.iceConnectionState}`
                );
            }
        };

    /*
     * WebRTC connection diagnostics.
     */
    pc.onconnectionstatechange =
        () => {

            console.log(
                `[SESSION ${sessionId}] connection:`,
                pc.connectionState,
                `ice=${pc.iceConnectionState}`,
                `signaling=${pc.signalingState}`
            );

            // Don't stop during initial setup - only stop on actual failure
            // "closed" can happen during normal ICE gathering, so ignore it initially
            if (
                pc.connectionState === "failed"
            ) {

                console.error(
                    `[SESSION ${sessionId}] Connection failed! This usually means:`,
                    `\n  1. No compatible ICE candidates found`,
                    `\n  2. Firewall blocking WebRTC`,
                    `\n  3. NAT traversal failed (need TURN server)`
                );

                stopSession(
                    sessionId,
                    `connection-${pc.connectionState}`
                );
            }
            
            if (
                pc.connectionState === "closed" &&
                !session.stopping
            ) {
                console.warn(
                    `[SESSION ${sessionId}] Connection closed unexpectedly!`,
                    `Stack trace:`,
                    new Error().stack
                );
            }
        };

    /*
     * Start RTSP -> I420.
     */
    session.ffmpeg =
        startWebRtcFfmpeg(
            sessionId,
            rtspUrl,
            videoSource
        );

    // Give FFmpeg a moment to start before creating offer
    // This prevents the PeerConnection from closing prematurely
    await new Promise(resolve => setTimeout(resolve, 500));

    // Check if session was stopped while waiting
    if (session.stopping) {
        console.log(
            `[SESSION ${sessionId}] Session stopped before offer creation`
        );
        return;
    }

    /*
     * Create WebRTC offer.
     */
    const offer =
        await pc.createOffer();

    await pc.setLocalDescription(
        offer
    );

    console.log(
        `[SESSION ${sessionId}] Offer created, waiting for ICE...`
    );

    /*
     * Non-trickle ICE.
     *
     * All candidates are included in
     * the SDP before publishing.
     */
    await waitForIceComplete(
        pc,
        15000
    );

    // Check if session was stopped during ICE gathering
    if (session.stopping) {
        console.log(
            `[SESSION ${sessionId}] Session was stopped during ICE gathering`
        );
        return;
    }

    const localDescription =
        pc.localDescription;

    if (
        !localDescription
    ) {

        throw new Error(
            "Local WebRTC description is missing"
        );
    }

    console.log(
        `[SESSION ${sessionId}] ICE gathering complete`
    );

    const localSdp = session.pc.localDescription.sdp;

    const localCandidates = localSdp
        .split("\n")
        .filter(line => line.startsWith("a=candidate:"));

    console.log(
        `[ICE OFFER CANDIDATES] session=${sessionId} total=${localCandidates.length}`
    );
    
    // Categorize candidates
    const hostCandidates = localCandidates.filter(c => c.includes("typ host"));
    const srflxCandidates = localCandidates.filter(c => c.includes("typ srflx"));
    const relayCandidates = localCandidates.filter(c => c.includes("typ relay"));
    
    console.log(
        `[ICE ANALYSIS] ` +
        `host=${hostCandidates.length} ` +
        `srflx=${srflxCandidates.length} ` +
        `relay=${relayCandidates.length}`
    );
    
    if (srflxCandidates.length === 0) {
        console.warn(
            `[WARNING] No srflx candidates! Internet streaming will likely fail. ` +
            `This means STUN is not working or Pi has no internet access.`
        );
    }
    
    console.log(`[ICE CANDIDATES DETAIL]:\n${localCandidates.join("\n") || "NONE"}`);
    
    if (localCandidates.length === 0) {
        console.error(
            `[CRITICAL ERROR] NO ICE CANDIDATES! ` +
            `Check network connectivity and firewall settings.`
        );
    }

    // Extract and log server's public IP candidates
    const serverSrflx = srflxCandidates[0];
    if (serverSrflx) {
        const ipMatch = serverSrflx.match(/(\d+\.\d+\.\d+\.\d+)/);
        if (ipMatch) {
            console.log(
                `[SERVER PUBLIC IP] ${ipMatch[1]} (via STUN)`
            );
        }
    } else {
        console.error(
            `[CRITICAL] Server has no public IP candidate! ` +
            `This means the Pi cannot be reached from internet. ` +
            `Possible causes:\n` +
            `  1. Pi's firewall blocking UDP ports\n` +
            `  2. Router/NAT not allowing UDP\n` +
            `  3. STUN servers unreachable from Pi\n` +
            `  4. Pi behind symmetric NAT (TURN required)`
        );
    }

    publishJson(
        topicFor(
            OFFER_TOPIC_PREFIX,
            sessionId
        ),
        {
            sessionId,

            type:
                localDescription.type,

            sdp:
                localDescription.sdp
        }
    );

    console.log(
        `[SESSION ${sessionId}] offer published`
    );
    
    // Log a sample of the SDP for debugging
    const sdpPreview = localDescription.sdp
        .split('\n')
        .slice(0, 20)
        .join('\n');
    
    console.log(
        `[SDP PREVIEW] First 20 lines:\n${sdpPreview}\n...`
    );
}

/* ================================================================
 * START WEBRTC REQUEST
 * ================================================================ */

async function handleStart(
    payload
) {

    const sessionId =
        safeString(
            payload.sessionId
        );

    const deviceId =
        safeString(
            payload.deviceId
        );

    if (
        !sessionId
    ) {

        throw new Error(
            "sessionId is required"
        );
    }

    if (
        !deviceId
    ) {

        throw new Error(
            "deviceId is required"
        );
    }

    console.log(
        `[REQUEST] start session=${sessionId} device=${deviceId}`
    );

    /*
     * IMPORTANT:
     *
     * Camera information is loaded locally.
     *
     * Do not trust credentials/IP received
     * through MQTT.
     */
    const camera =
        getCamera(
            deviceId
        );

    if (
        !camera
    ) {

        throw new Error(
            `Camera not registered: ${deviceId}`
        );
    }

    const requestedChannel =
        payload.channel !== undefined
            ? Number(
                payload.channel
            )
            : camera.channel;

    const requestedSubtype =
        payload.subtype !== undefined
            ? Number(
                payload.subtype
            )
            : camera.subtype;

    const effectiveCamera = {

        ...camera,

        channel:
            Number.isFinite(
                requestedChannel
            )
                ? requestedChannel
                : camera.channel,

        subtype:
            Number.isFinite(
                requestedSubtype
            )
                ? requestedSubtype
                : camera.subtype
    };

    await createWebRtcSession(
        sessionId,
        effectiveCamera
    );
}

/* ================================================================
 * WEBRTC ANSWER
 * ================================================================ */

async function handleAnswer(
    payload
) {

    console.log(
        "[ANSWER RAW]",
        JSON.stringify(
            payload,
            null,
            2
        )
    );

    const sessionId =
        safeString(
            payload.sessionId
        );

    const session =
        sessions.get(
            sessionId
        );

    if (
        !session
    ) {

        console.warn(
            `[ANSWER] session not found: ${sessionId}`
        );

        return;
    }

    console.log(
        `[ANSWER] type=${payload.type || "MISSING"}`
    );

    console.log(
        `[ANSWER] sdp length=${payload.sdp ? payload.sdp.length : 0}`
    );

    if (
        !payload.sdp ||
        !payload.type
    ) {

        console.error(
            "[ANSWER] Invalid answer received:",
            JSON.stringify(
                payload
            )
        );

        return;
    }

    // Extract and log browser's ICE candidates from answer
    const answerCandidates = payload.sdp
        .split('\n')
        .filter(line => line.includes('a=candidate:'));
    
    const browserHost = answerCandidates.filter(c => c.includes('typ host'));
    const browserSrflx = answerCandidates.filter(c => c.includes('typ srflx'));
    const browserRelay = answerCandidates.filter(c => c.includes('typ relay'));
    
    console.log(
        `[ANSWER ICE ANALYSIS] Browser candidates: ` +
        `total=${answerCandidates.length} ` +
        `host=${browserHost.length} ` +
        `srflx=${browserSrflx.length} ` +
        `relay=${browserRelay.length}`
    );
    
    if (browserRelay.length === 0) {
        console.warn(
            `[ANSWER] Browser has NO relay candidates! ` +
            `TURN may not be configured correctly in browser.`
        );
    }
    
    console.log(
        `[ANSWER CANDIDATES]:\n${answerCandidates.join('\n') || 'NONE'}`
    );

    const answer =
        new RTCSessionDescription({
            type:
                payload.type,

            sdp:
                payload.sdp
        });

    try {

        await session.pc.setRemoteDescription(
            answer
        );

        console.log(
            `[SESSION ${sessionId}] remote description applied`
        );

    } catch (error) {

        console.error(
            `[SESSION ${sessionId}] setRemoteDescription failed:`,
            error
        );
    }
}

/* ================================================================
 * STOP WEBRTC REQUEST
 * ================================================================ */

function handleStop(
    payload
) {

    const sessionId =
        safeString(
            payload.sessionId
        );

    if (
        !sessionId
    ) {

        return;
    }

    stopSession(
        sessionId,
        "client-stop"
    );
}

/* ================================================================
 * SAFE CAMERA OBJECT
 * ================================================================ */

function safeCameraForBrowser(
    camera
) {

    /*
     * NEVER expose:
     *
     * username
     * password
     * IP
     * RTSP URL
     */
    return {

        deviceId:
            camera.deviceId,

        type:
            camera.type,

        manufacturer:
            camera.manufacturer,

        model:
            camera.model,

        channel:
            Number(
                camera.channel || 1
            ),

        subtype:
            Number(
                camera.subtype ?? 0
            ),

        rtsp:
            Boolean(
                camera.rtsp
            ),

        onvif:
            Boolean(
                camera.onvif
            )
    };
}

/* ================================================================
 * CAMERA LIST
 * ================================================================ */

function handleCameraListRequest(
    payload
) {

    const sessionId =
        safeString(
            payload.sessionId
        );

    if (
        !sessionId
    ) {

        console.warn(
            "[CAMERA LIST] missing sessionId"
        );

        return;
    }

    const cameras =
        listCameras()
            .map(
                safeCameraForBrowser
            );

    publishJson(
        topicFor(
            CAMERA_LIST_RESPONSE_PREFIX,
            sessionId
        ),
        {
            sessionId,
            cameras
        }
    );

    console.log(
        `[CAMERA LIST] sent ${cameras.length} cameras to ${sessionId}`
    );
}

/* ================================================================
 * MQTT MESSAGE HANDLER
 * ================================================================ */

async function handleMqttMessage(
    topic,
    message
) {

    let payload;

    try {

        payload =
            JSON.parse(
                message.toString()
            );

    } catch (error) {

        console.error(
            "[MQTT] Invalid JSON:",
            error.message
        );

        return;
    }

    try {

        /*
         * START / STOP
         */
        if (
            topic ===
            REQUEST_TOPIC
        ) {

            if (
                payload.action ===
                "start"
            ) {

                await handleStart(
                    payload
                );

            } else if (
                payload.action ===
                "stop"
            ) {

                handleStop(
                    payload
                );

            } else {

                console.warn(
                    "[MQTT] Unknown action:",
                    payload.action
                );
            }

            return;
        }

        /*
         * ANSWER
         */
        if (
            topic.startsWith(
                ANSWER_TOPIC_PREFIX + "/"
            )
        ) {

            await handleAnswer(
                payload
            );

            return;
        }

        /*
         * CAMERA LIST
         */
        if (
            topic ===
            CAMERA_LIST_REQUEST_TOPIC
        ) {

            handleCameraListRequest(
                payload
            );

            return;
        }

    } catch (error) {

        console.error(
            "[MQTT] Request error:",
            error
        );

        const sessionId =
            safeString(
                payload.sessionId
            );

        if (
            sessionId
        ) {

            publishJson(
                topicFor(
                    OFFER_TOPIC_PREFIX,
                    sessionId
                ),
                {
                    sessionId,

                    type:
                        "error",

                    error:
                        error.message ||
                        "WebRTC server error"
                }
            );

            stopSession(
                sessionId,
                "request-error"
            );
        }
    }
}

/* ================================================================
 * MQTT CLIENT
 * ================================================================ */

mqttClient =
    mqtt.connect(
        MQTT_URL,
        {

            username:
                MQTT_USERNAME,

            password:
                MQTT_PASSWORD,

            clientId:
                `smartnode-webrtc-pi-${process.pid}-${Date.now()}`,

            clean:
                true,

            reconnectPeriod:
                3000,

            connectTimeout:
                10000,

            keepalive:
                30
        }
    );

mqttClient.on(
    "connect",
    () => {

        console.log(
            "================================="
        );

        console.log(
            " SMARTNODE CAMERA SERVER"
        );

        console.log(
            " LAN RTSP + INTERNET WEBRTC"
        );

        console.log(
            "================================="
        );

        console.log(
            "[MQTT] Connected:",
            MQTT_URL
        );

        mqttClient.subscribe(
            [
                REQUEST_TOPIC,

                `${ANSWER_TOPIC_PREFIX}/+`,

                CAMERA_LIST_REQUEST_TOPIC
            ],
            {
                qos: 0
            },
            error => {

                if (
                    error
                ) {

                    console.error(
                        "[MQTT] Subscribe error:",
                        error.message
                    );

                    return;
                }

                console.log(
                    "[MQTT] Subscribed:"
                );

                console.log(
                    " ",
                    REQUEST_TOPIC
                );

                console.log(
                    " ",
                    `${ANSWER_TOPIC_PREFIX}/+`
                );

                console.log(
                    " ",
                    CAMERA_LIST_REQUEST_TOPIC
                );
            }
        );
    }
);

mqttClient.on(
    "reconnect",
    () => {

        console.log(
            "[MQTT] Reconnecting..."
        );
    }
);

mqttClient.on(
    "close",
    () => {

        console.log(
            "[MQTT] Connection closed"
        );
    }
);

mqttClient.on(
    "offline",
    () => {

        console.log(
            "[MQTT] Offline"
        );
    }
);

mqttClient.on(
    "error",
    error => {

        console.error(
            "[MQTT] Error:",
            error.message
        );
    }
);

mqttClient.on(
    "message",
    (
        topic,
        message
    ) => {

        handleMqttMessage(
            topic,
            message
        ).catch(
            error => {

                console.error(
                    "[MQTT] Handler error:",
                    error
                );
            }
        );
    }
);

/* ================================================================
 * RTSP VERIFICATION
 * ================================================================ */

function verifyRtsp(
    device,
    username,
    password
) {

    return new Promise(
        resolve => {

            let finished =
                false;

            const camera = {

                ...device,

                credentials: {
                    username,
                    password
                }
            };

            let rtspUrl;

            try {

                rtspUrl =
                    buildRtspUrl(
                        camera
                    );

            } catch (error) {

                resolve({
                    success:
                        false,

                    error:
                        error.message
                });

                return;
            }

            console.log(
                "[VERIFY] Testing:",
                redactRtspUrl(
                    rtspUrl
                )
            );

            const args = [

                "-hide_banner",

                "-loglevel",
                "error",

                "-rtsp_transport",
                "tcp",

                "-i",
                rtspUrl,

                "-t",
                "2",

                "-f",
                "null",

                "-"
            ];

            const ffmpeg =
                spawn(
                    "ffmpeg",
                    args,
                    {
                        stdio: [
                            "ignore",
                            "ignore",
                            "pipe"
                        ]
                    }
                );

            let stderr = "";

            function finish(
                result
            ) {

                if (
                    finished
                ) {

                    return;
                }

                finished =
                    true;

                try {

                    ffmpeg.kill(
                        "SIGKILL"
                    );

                } catch (error) {}

                resolve(
                    result
                );
            }

            ffmpeg.stderr.on(
                "data",
                data => {

                    stderr +=
                        data.toString();

                    if (
                        stderr.length >
                        5000
                    ) {

                        stderr =
                            stderr.slice(
                                -5000
                            );
                    }
                }
            );

            ffmpeg.on(
                "error",
                error => {

                    finish({
                        success:
                            false,

                        error:
                            error.message
                    });
                }
            );

            ffmpeg.on(
                "exit",
                (
                    code,
                    signal
                ) => {

                    if (
                        code === 0
                    ) {

                        finish({
                            success:
                                true
                        });

                    } else {

                        finish({
                            success:
                                false,

                            error:
                                stderr.trim() ||
                                `FFmpeg exited with code ${code} signal ${signal}`
                        });
                    }
                }
            );

            setTimeout(
                () => {

                    finish({
                        success:
                            false,

                        error:
                            "RTSP verification timeout"
                    });

                },
                8000
            );
        }
    );
}

/* ================================================================
 * INTERNAL HTTP SERVER :8090
 * ================================================================ */

const internalServer =
    http.createServer(
        (
            req,
            res
        ) => {

            /*
             * Health.
             */
            if (
                req.method === "GET" &&
                req.url === "/health"
            ) {

                res.writeHead(
                    200,
                    {
                        "Content-Type":
                            "application/json"
                    }
                );

                res.end(
                    JSON.stringify({

                        success:
                            true,

                        service:
                            "webrtc-camera-server",

                        mqtt:
                            mqttClient.connected,

                        sessions:
                            sessions.size,

                        lanStreams:
                            lanStreams.size
                    })
                );

                return;
            }

            /*
             * Only POST /internal/camera/verify.
             */
            if (
                req.method !== "POST" ||
                req.url !==
                    "/internal/camera/verify"
            ) {

                res.writeHead(
                    404,
                    {
                        "Content-Type":
                            "application/json"
                    }
                );

                res.end(
                    JSON.stringify({

                        success:
                            false,

                        error:
                            "Not found"
                    })
                );

                return;
            }

            let body = "";

            req.on(
                "data",
                chunk => {

                    body +=
                        chunk.toString();

                    /*
                     * Prevent huge body.
                     */
                    if (
                        body.length >
                        1024 * 1024
                    ) {

                        req.destroy();
                    }
                }
            );

            req.on(
                "end",
                async () => {

                    try {

                        const payload =
                            JSON.parse(
                                body
                            );

                        const device =
                            payload.device;

                        const username =
                            safeString(
                                payload.username
                            );

                        const password =
                            safeString(
                                payload.password
                            );

                        if (
                            !device ||
                            !device.ip
                        ) {

                            throw new Error(
                                "device.ip is required"
                            );
                        }

                        if (
                            !username ||
                            !password
                        ) {

                            throw new Error(
                                "username and password are required"
                            );
                        }

                        const result =
                            await verifyRtsp(
                                device,
                                username,
                                password
                            );

                        res.writeHead(
                            result.success
                                ? 200
                                : 400,
                            {
                                "Content-Type":
                                    "application/json"
                            }
                        );

                        res.end(
                            JSON.stringify(
                                result
                            )
                        );

                    } catch (error) {

                        res.writeHead(
                            400,
                            {
                                "Content-Type":
                                    "application/json"
                            }
                        );

                        res.end(
                            JSON.stringify({

                                success:
                                    false,

                                error:
                                    error.message
                            })
                        );
                    }
                }
            );
        }
    );

/* ================================================================
 * LAN MJPEG HELPERS
 * ================================================================ */

function writeLanJson(
    res,
    statusCode,
    data
) {

    res.writeHead(
        statusCode,
        {
            "Content-Type":
                "application/json",

            "Cache-Control":
                "no-store",

            "Access-Control-Allow-Origin":
                "*"
        }
    );

    res.end(
        JSON.stringify(
            data
        )
    );
}

function getLanStreamKey(
    deviceId,
    channel,
    subtype
) {

    return [
        deviceId,
        Number(
            channel || 1
        ),
        Number(
            subtype ?? 0
        )
    ].join(":");
}

/* ================================================================
 * JPEG PARSING
 * ================================================================ */

function findJpegStart(
    buffer
) {

    for (
        let i = 0;
        i < buffer.length - 1;
        i++
    ) {

        if (
            buffer[i] === 0xff &&
            buffer[i + 1] === 0xd8
        ) {

            return i;
        }
    }

    return -1;
}

function findJpegEnd(
    buffer,
    start
) {

    for (
        let i = start;
        i < buffer.length - 1;
        i++
    ) {

        if (
            buffer[i] === 0xff &&
            buffer[i + 1] === 0xd9
        ) {

            return i;
        }
    }

    return -1;
}

/* ================================================================
 * LAN STREAM
 * ================================================================ */

function startLanStream(
    device,
    channel,
    subtype
) {

    const effectiveCamera = {

        ...device,

        channel:
            Number(
                channel ||
                device.channel ||
                1
            ),

        subtype:
            Number(
                subtype ??
                device.subtype ??
                0
            )
    };

    const key =
        getLanStreamKey(
            effectiveCamera.deviceId,
            effectiveCamera.channel,
            effectiveCamera.subtype
        );

    /*
     * If another browser tab is already watching
     * this camera, reuse the FFmpeg process.
     */
    const existing =
        lanStreams.get(
            key
        );

    if (
        existing
    ) {

        return existing;
    }

    const rtspUrl =
        buildRtspUrl(
            effectiveCamera
        );

    console.log(
        `[LAN STREAM ${key}] RTSP:`,
        redactRtspUrl(
            rtspUrl
        )
    );

    /*
     * RTSP -> MJPEG
     */
    const args = [

        "-hide_banner",

        "-loglevel",
        "warning",

        "-rtsp_transport",
        "tcp",

        "-i",
        rtspUrl,

        "-an",

        "-vf",
        `scale=${WIDTH}:${HEIGHT}:force_original_aspect_ratio=decrease,pad=${WIDTH}:${HEIGHT}:(ow-iw)/2:(oh-ih)/2`,

        "-r",
        "15",

        "-q:v",
        "5",

        "-f",
        "mjpeg",

        "pipe:1"
    ];

    const ffmpeg =
        spawn(
            "ffmpeg",
            args,
            {
                stdio: [
                    "ignore",
                    "pipe",
                    "pipe"
                ]
            }
        );

    const stream = {

        key,

        deviceId:
            effectiveCamera.deviceId,

        camera:
            effectiveCamera,

        ffmpeg,

        clients:
            new Set(),

        buffer:
            Buffer.alloc(0),

        frameCount:
            0,

        stopping:
            false,

        startedAt:
            Date.now()
    };

    lanStreams.set(
        key,
        stream
    );

    /*
     * FFmpeg stdout contains JPEG frames.
     */
    ffmpeg.stdout.on(
        "data",
        chunk => {

            if (
                stream.stopping
            ) {

                return;
            }

            stream.buffer =
                Buffer.concat([
                    stream.buffer,
                    chunk
                ]);

            while (true) {

                const start =
                    findJpegStart(
                        stream.buffer
                    );

                if (
                    start < 0
                ) {

                    /*
                     * Prevent unlimited buffer growth.
                     */
                    if (
                        stream.buffer.length >
                        1024 * 1024
                    ) {

                        stream.buffer =
                            stream.buffer.slice(
                                -65536
                            );
                    }

                    break;
                }

                const end =
                    findJpegEnd(
                        stream.buffer,
                        start + 2
                    );

                if (
                    end < 0
                ) {

                    break;
                }

                const frame =
                    stream.buffer.slice(
                        start,
                        end + 2
                    );

                stream.buffer =
                    stream.buffer.slice(
                        end + 2
                    );

                stream.frameCount++;

                broadcastMjpegFrame(
                    stream,
                    frame
                );
            }
        }
    );

    ffmpeg.stderr.on(
        "data",
        data => {

            const text =
                data
                    .toString()
                    .trim();

            if (
                text
            ) {

                console.log(
                    `[LAN FFMPEG ${key}] ${text}`
                );
            }
        }
    );

    ffmpeg.on(
        "error",
        error => {

            console.error(
                `[LAN STREAM ${key}] FFmpeg error:`,
                error.message
            );

            stopLanStream(
                key,
                "ffmpeg-error"
            );
        }
    );

    ffmpeg.on(
        "exit",
        (
            code,
            signal
        ) => {

            console.log(
                `[LAN STREAM ${key}] FFmpeg exited code=${code} signal=${signal}`
            );

            stopLanStream(
                key,
                "ffmpeg-exited"
            );
        }
    );

    return stream;
}

/* ================================================================
 * BROADCAST MJPEG FRAME
 * ================================================================ */

function broadcastMjpegFrame(
    stream,
    frame
) {

    const header =
        Buffer.from(
            "--frame\r\n" +
            "Content-Type: image/jpeg\r\n" +
            `Content-Length: ${frame.length}\r\n` +
            "Cache-Control: no-cache\r\n" +
            "\r\n"
        );

    const ending =
        Buffer.from(
            "\r\n"
        );

    const packet =
        Buffer.concat([
            header,
            frame,
            ending
        ]);

    for (
        const client
        of stream.clients
    ) {

        try {

            if (
                client.destroyed
            ) {

                stream.clients.delete(
                    client
                );

                continue;
            }

            client.write(
                packet
            );

        } catch (error) {

            stream.clients.delete(
                client
            );

            try {
                client.destroy();
            } catch (e) {}
        }
    }
}

/* ================================================================
 * STOP LAN STREAM
 * ================================================================ */

function stopLanStream(
    key,
    reason = "stopped"
) {

    const stream =
        lanStreams.get(
            key
        );

    if (
        !stream
    ) {

        return;
    }

    if (
        stream.stopping
    ) {

        return;
    }

    stream.stopping =
        true;

    console.log(
        `[LAN STREAM ${key}] stopping: ${reason}`
    );

    if (
        stream.ffmpeg
    ) {

        try {

            stream.ffmpeg.kill(
                "SIGKILL"
            );

        } catch (error) {}
    }

    for (
        const client
        of stream.clients
    ) {

        try {
            client.end();
        } catch (error) {}

        try {
            client.destroy();
        } catch (error) {}
    }

    stream.clients.clear();

    lanStreams.delete(
        key
    );
}

/* ================================================================
 * LAN STREAM CLEANUP
 * ================================================================ */

function scheduleLanStreamCleanup(
    key
) {

    /*
     * Give the browser a few seconds to reconnect
     * before killing FFmpeg.
     */
    setTimeout(
        () => {

            const stream =
                lanStreams.get(
                    key
                );

            if (
                !stream
            ) {

                return;
            }

            if (
                stream.clients.size === 0
            ) {

                stopLanStream(
                    key,
                    "no-clients"
                );
            }

        },
        5000
    );
}

/* ================================================================
 * LAN MJPEG REQUEST
 * ================================================================ */

async function handleLanMjpegRequest(
    req,
    res
) {

    try {

        const parsed =
            new URL(
                req.url,
                "http://localhost"
            );

        const prefix =
            "/local/mjpeg/";

        const deviceId =
            decodeURIComponent(
                parsed.pathname.substring(
                    prefix.length
                )
            );

        if (
            !deviceId
        ) {

            writeLanJson(
                res,
                400,
                {
                    success:
                        false,

                    error:
                        "deviceId is required"
                }
            );

            return;
        }

        /*
         * Load camera from encrypted configuration.
         */
        const camera =
            getCamera(
                deviceId
            );

        if (
            !camera
        ) {

            writeLanJson(
                res,
                404,
                {
                    success:
                        false,

                    error:
                        "Camera not registered"
                }
            );

            return;
        }

        const channel =
            Number(
                parsed.searchParams.get(
                    "channel"
                ) ||
                camera.channel ||
                1
            );

        const subtypeParam =
            parsed.searchParams.get(
                "subtype"
            );

        const subtype =
            subtypeParam !== null
                ? Number(
                    subtypeParam
                )
                : Number(
                    camera.subtype ??
                    0
                );

        const stream =
            startLanStream(
                camera,
                channel,
                subtype
            );

        /*
         * Multipart MJPEG response.
         */
        res.writeHead(
            200,
            {

                "Content-Type":
                    "multipart/x-mixed-replace; boundary=frame",

                "Cache-Control":
                    "no-cache, no-store, must-revalidate",

                "Pragma":
                    "no-cache",

                "Expires":
                    "0",

                "Connection":
                    "close",

                "Access-Control-Allow-Origin":
                    "*"
            }
        );

        stream.clients.add(
            res
        );

        console.log(
            `[LAN STREAM ${stream.key}] client connected. clients=${stream.clients.size}`
        );

        function removeClient() {

            if (
                stream.clients.has(
                    res
                )
            ) {

                stream.clients.delete(
                    res
                );

                console.log(
                    `[LAN STREAM ${stream.key}] client disconnected. clients=${stream.clients.size}`
                );

                scheduleLanStreamCleanup(
                    stream.key
                );
            }
        }

        req.on(
            "close",
            removeClient
        );

        res.on(
            "close",
            removeClient
        );

        res.on(
            "error",
            removeClient
        );

    } catch (error) {

        console.error(
            "[LAN MJPEG] error:",
            error
        );

        if (
            !res.headersSent
        ) {

            writeLanJson(
                res,
                500,
                {

                    success:
                        false,

                    error:
                        error.message
                }
            );

        } else {

            try {
                res.end();
            } catch (e) {}
        }
    }
}

/* ================================================================
 * LAN HTTP SERVER :8091
 * ================================================================ */

const lanStreamServer =
    http.createServer(
        async (
            req,
            res
        ) => {

            /*
             * Only LAN/private clients.
             */
            if (
                !isAllowedLanClient(
                    req
                )
            ) {

                console.warn(
                    "[LAN HTTP] rejected:",
                    req.socket.remoteAddress
                );

                writeLanJson(
                    res,
                    403,
                    {

                        success:
                            false,

                        error:
                            "LAN access only"
                    }
                );

                return;
            }

            /*
             * Health.
             */
            if (
                req.method === "GET" &&
                req.url === "/health"
            ) {

                writeLanJson(
                    res,
                    200,
                    {

                        success:
                            true,

                        service:
                            "lan-camera-stream",

                        streams:
                            lanStreams.size
                    }
                );

                return;
            }

            /*
             * Safe camera list.
             */
            if (
                req.method === "GET" &&
                req.url === "/local/cameras"
            ) {

                const cameras =
                    listCameras()
                        .map(
                            safeCameraForBrowser
                        );

                writeLanJson(
                    res,
                    200,
                    {

                        success:
                            true,

                        cameras
                    }
                );

                return;
            }

            /*
             * LAN MJPEG.
             */
            if (
                req.method === "GET" &&
                req.url.startsWith(
                    "/local/mjpeg/"
                )
            ) {

                await handleLanMjpegRequest(
                    req,
                    res
                );

                return;
            }

            /*
             * Not found.
             */
            writeLanJson(
                res,
                404,
                {

                    success:
                        false,

                    error:
                        "Not found"
                }
            );
        }
    );

/* ================================================================
 * START INTERNAL SERVER
 * ================================================================ */

internalServer.listen(
    INTERNAL_PORT,
    INTERNAL_HOST,
    () => {

        console.log(
            `[HTTP] Internal server listening on ${INTERNAL_HOST}:${INTERNAL_PORT}`
        );

        console.log(
            "[HTTP] Camera verification:"
        );

        console.log(
            `      http://${INTERNAL_HOST}:${INTERNAL_PORT}/internal/camera/verify`
        );
    }
);

/* ================================================================
 * START LAN SERVER
 * ================================================================ */

lanStreamServer.listen(
    LAN_STREAM_PORT,
    LAN_STREAM_HOST,
    () => {

        console.log(
            `[LAN] Camera streaming server listening on ${LAN_STREAM_HOST}:${LAN_STREAM_PORT}`
        );

        console.log(
            "[LAN] Camera list:"
        );

        console.log(
            `      http://<PI-IP>:${LAN_STREAM_PORT}/local/cameras`
        );

        console.log(
            "[LAN] MJPEG:"
        );

        console.log(
            `      http://<PI-IP>:${LAN_STREAM_PORT}/local/mjpeg/<deviceId>`
        );
    }
);

/* ================================================================
 * SHUTDOWN
 * ================================================================ */

function shutdown(
    signal
) {

    console.log(
        `[SHUTDOWN] ${signal}`
    );

    /*
     * Stop WebRTC sessions.
     */
    for (
        const sessionId
        of sessions.keys()
    ) {

        stopSession(
            sessionId,
            signal
        );
    }

    /*
     * Stop LAN streams.
     */
    for (
        const key
        of lanStreams.keys()
    ) {

        stopLanStream(
            key,
            signal
        );
    }

    /*
     * Close LAN server.
     */
    try {

        lanStreamServer.close(
            () => {

                console.log(
                    "[LAN] Server stopped"
                );
            }
        );

    } catch (error) {}

    /*
     * Close MQTT.
     */
    try {

        mqttClient.end(
            true,
            () => {

                console.log(
                    "[MQTT] Disconnected"
                );

                /*
                 * Close internal server.
                 */
                try {

                    internalServer.close(
                        () => {

                            console.log(
                                "[HTTP] Internal server stopped"
                            );

                            process.exit(
                                0
                            );
                        }
                    );

                } catch (error) {

                    process.exit(
                        0
                    );
                }
            }
        );

    } catch (error) {

        process.exit(
            0
        );
    }

    /*
     * Safety exit.
     */
    setTimeout(
        () => {

            process.exit(
                0
            );

        },
        3000
    );
}

/* ================================================================
 * PROCESS SIGNALS
 * ================================================================ */

process.on(
    "SIGINT",
    () => {

        shutdown(
            "SIGINT"
        );
    }
);

process.on(
    "SIGTERM",
    () => {

        shutdown(
            "SIGTERM"
        );
    }
);

process.on(
    "uncaughtException",
    error => {

        console.error(
            "[PROCESS] uncaughtException:",
            error
        );
    }
);

process.on(
    "unhandledRejection",
    error => {

        console.error(
            "[PROCESS] unhandledRejection:",
            error
        );
    }
);

/* ================================================================
 * STARTUP
 * ================================================================ */

console.log(
    "================================================"
);

console.log(
    " SmartNode Camera Server"
);

console.log(
    " LAN RTSP + INTERNET WEBRTC"
);

console.log(
    "================================================"
);

console.log(
    `[CONFIG] Internal HTTP: ${INTERNAL_HOST}:${INTERNAL_PORT}`
);

console.log(
    `[CONFIG] LAN HTTP: ${LAN_STREAM_HOST}:${LAN_STREAM_PORT}`
);

console.log(
    `[CONFIG] MQTT: ${MQTT_URL}`
);

console.log(
    "[CONFIG] Server starting..."
);
