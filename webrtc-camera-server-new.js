/**
 * SmartNode MQTT + WebRTC Camera/NVR Server
 *
 * Browser
 *   |
 *   | MQTT start(deviceId)
 *   v
 * MQTT Broker
 *   |
 *   v
 * Raspberry Pi
 *   |
 *   | RTSP
 *   v
 * Camera/NVR
 *
 * Raspberry Pi
 *   |
 *   | WebRTC video
 *   v
 * Browser
 *
 * Camera credentials remain on Raspberry Pi.
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

/* ============================================================
 * CONFIG
 * ============================================================ */

const MQTT_URL =
    process.env.MQTT_URL ||
    "tcp://connect.smartnode.in:1883";

const MQTT_USERNAME =
    process.env.MQTT_USERNAME || "";

const MQTT_PASSWORD =
    process.env.MQTT_PASSWORD || "";

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

const INTERNAL_HOST =
    process.env.WEBRTC_INTERNAL_HOST ||
    "127.0.0.1";

const INTERNAL_PORT =
    Number(
        process.env.WEBRTC_INTERNAL_PORT || 8090
    );

const WIDTH = 640;
const HEIGHT = 360;
const FPS = 25;

const FRAME_SIZE =
    WIDTH * HEIGHT +
    (WIDTH / 2) * (HEIGHT / 2) +
    (WIDTH / 2) * (HEIGHT / 2);

/*
 * STUN only for now.
 *
 * TURN can be added later if NAT traversal
 * fails on some networks.
 */
const ICE_SERVERS = [
    {
        urls: "stun:stun.l.google.com:19302"
    },
    {
        urls: "stun:stun1.l.google.com:19302"
    },
    {
        urls: "stun:stun2.l.google.com:19302"
    },
    {
        urls: "stun:stun3.l.google.com:19302"
    }
];

const sessions = new Map();

/* ============================================================
 * HELPERS
 * ============================================================ */

function safeString(value) {
    return value === undefined ||
        value === null
        ? ""
        : String(value);
}

function topicFor(prefix, sessionId) {
    return `${prefix}/${sessionId}`;
}

function redactRtspUrl(url) {
    return String(url || "").replace(
        /:\/\/([^:@/]+):([^@/]+)@/g,
        "://***:***@"
    );
}

/* ============================================================
 * RTSP URL
 * ============================================================ */

function buildRtspUrl(device) {

    if (!device || !device.ip) {
        throw new Error(
            "Camera IP is missing"
        );
    }

    /*
     * Credentials are ONLY read from the
     * encrypted camera configuration.
     */
    const username =
        device.credentials?.username;

    const password =
        device.credentials?.password;

    if (!username || !password) {
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
    if (device.rtspUrl) {

        return String(device.rtspUrl)
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
     * Dahua / NVR RTSP.
     *
     * Important:
     * Some discovered devices have
     * type="camera", therefore rtsp===true
     * is also checked.
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
            `/cam/realmonitor?channel=${channel}` +
            `&subtype=${subtype}`
        );
    }

    /*
     * Generic configured RTSP path.
     */
    if (device.rtspPath) {

        const path =
            String(device.rtspPath)
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

/* ============================================================
 * ICE GATHERING
 * ============================================================ */

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

    return new Promise(resolve => {

        let finished = false;
        let timer;

        const cleanup = () => {

            clearTimeout(timer);

            pc.removeEventListener(
                "icegatheringstatechange",
                check
            );
        };

        const finish = () => {

            if (finished) {
                return;
            }

            finished = true;

            cleanup();

            resolve();
        };

        const check = () => {

            if (
                pc.iceGatheringState ===
                "complete"
            ) {
                finish();
            }
        };

        timer = setTimeout(
            finish,
            timeoutMs
        );

        pc.addEventListener(
            "icegatheringstatechange",
            check
        );

        check();
    });
}

/* ============================================================
 * MQTT PUBLISH
 * ============================================================ */

function publishJson(
    topic,
    payload
) {

    const message =
        JSON.stringify(payload);

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
                    "MQTT publish error:",
                    error.message
                );
            }
        }
    );
}

/* ============================================================
 * SESSION CLEANUP
 * ============================================================ */

function stopSession(
    sessionId,
    reason = "stopped"
) {

    const session =
        sessions.get(sessionId);

    if (!session) {
        return;
    }

    console.log(
        `[SESSION ${sessionId}] stopping: ${reason}`
    );

    if (session.ffmpeg) {

        try {
            session.ffmpeg.kill(
                "SIGKILL"
            );
        } catch (e) {}
    }

    if (session.pc) {

        try {
            session.pc.close();
        } catch (e) {}
    }

    if (
        session.videoSource &&
        session.videoTrack
    ) {

        try {
            session.videoTrack.stop();
        } catch (e) {}
    }

    sessions.delete(
        sessionId
    );
}

/* ============================================================
 * FFmpeg
 * ============================================================ */

function startFfmpeg(
    sessionId,
    rtspUrl,
    videoSource
) {

    console.log(
        `[SESSION ${sessionId}] RTSP:`,
        redactRtspUrl(rtspUrl)
    );

    /*
     * RTSP -> raw YUV420P
     *
     * We deliberately use TCP for RTSP because
     * it is more reliable across LAN/NVR setups.
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
        String(FPS),

        "-pix_fmt",
        "yuv420p",

        "-f",
        "rawvideo",

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

    let buffer =
        Buffer.alloc(0);

    ffmpeg.stdout.on(
        "data",
        chunk => {

            buffer =
                Buffer.concat([
                    buffer,
                    chunk
                ]);

            while (
                buffer.length >=
                FRAME_SIZE
            ) {

                const frame =
                    buffer.subarray(
                        0,
                        FRAME_SIZE
                    );

                buffer =
                    buffer.subarray(
                        FRAME_SIZE
                    );

                try {

                    videoSource.onFrame({
                        width: WIDTH,
                        height: HEIGHT,
                        data: frame
                    });

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
                data.toString().trim();

            if (text) {

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
                `[SESSION ${sessionId}] FFmpeg error:`,
                error.message
            );
        }
    );

    ffmpeg.on(
        "exit",
        (code, signal) => {

            console.log(
                `[SESSION ${sessionId}] FFmpeg exited code=${code} signal=${signal}`
            );

            const session =
                sessions.get(sessionId);

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
/* ============================================================
 * CREATE WEBRTC SESSION
 * ============================================================ */

async function createWebRtcSession(
    sessionId,
    camera
) {

    if (
        sessions.has(sessionId)
    ) {
        stopSession(
            sessionId,
            "duplicate-session"
        );
    }

    const rtspUrl =
        buildRtspUrl(camera);

    console.log(
        `[SESSION ${sessionId}] starting camera ${camera.ip}`
    );

    const pc =
        new RTCPeerConnection({
            iceServers: ICE_SERVERS,
            iceCandidatePoolSize: 10
        });

    const videoSource =
        new RTCVideoSource();

    const videoTrack =
        videoSource.createTrack();

    /*
     * Send video only.
     */
    pc.addTransceiver(
        videoTrack,
        {
            direction: "sendonly"
        }
    );

    const session = {
        sessionId,
        cameraId: camera.deviceId,
        pc,
        videoSource,
        videoTrack,
        ffmpeg: null,
        stopping: false,
        createdAt: Date.now()
    };

    sessions.set(
        sessionId,
        session
    );

    /* --------------------------------------------------------
     * ICE diagnostics
     * -------------------------------------------------------- */

    pc.onicegatheringstatechange = () => {

        console.log(
            `[SESSION ${sessionId}] ICE gathering:`,
            pc.iceGatheringState
        );
    };

    pc.oniceconnectionstatechange = () => {

        console.log(
            `[SESSION ${sessionId}] ICE connection:`,
            pc.iceConnectionState
        );

        if (
            pc.iceConnectionState ===
                "failed" ||
            pc.iceConnectionState ===
                "closed"
        ) {

            stopSession(
                sessionId,
                `ice-${pc.iceConnectionState}`
            );
        }
    };

    pc.onconnectionstatechange = () => {

        console.log(
            `[SESSION ${sessionId}] connection:`,
            pc.connectionState
        );

        if (
            pc.connectionState ===
                "failed" ||
            pc.connectionState ===
                "closed"
        ) {

            stopSession(
                sessionId,
                `connection-${pc.connectionState}`
            );
        }
    };

    /*
     * Start FFmpeg before sending offer.
     */
    session.ffmpeg =
        startFfmpeg(
            sessionId,
            rtspUrl,
            videoSource
        );

    /*
     * Create offer.
     */
    const offer =
        await pc.createOffer();

    await pc.setLocalDescription(
        offer
    );

    /*
     * Non-trickle ICE.
     *
     * Wait until all candidates are included
     * inside the SDP.
     */
    await waitForIceComplete(
        pc,
        15000
    );

    const localDescription =
        pc.localDescription;

    if (!localDescription) {
        throw new Error(
            "Local WebRTC description is missing"
        );
    }

    console.log(
        `[SESSION ${sessionId}] ICE gathering complete`
    );

    /*
     * Send offer to browser.
     */
    publishJson(
        topicFor(
            OFFER_TOPIC_PREFIX,
            sessionId
        ),
        {
            sessionId,
            type: localDescription.type,
            sdp: localDescription.sdp
        }
    );

    console.log(
        `[SESSION ${sessionId}] offer published`
    );
}

/* ============================================================
 * START REQUEST
 * ============================================================ */

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

    if (!sessionId) {

        throw new Error(
            "sessionId is required"
        );
    }

    if (!deviceId) {

        throw new Error(
            "deviceId is required"
        );
    }

    console.log(
        `[REQUEST] start session=${sessionId} device=${deviceId}`
    );

    /*
     * Camera is loaded ONLY from local encrypted
     * configuration.
     */
    const camera =
        getCamera(deviceId);

    if (!camera) {

        throw new Error(
            `Camera not registered: ${deviceId}`
        );
    }

    /*
     * MQTT may specify channel/subtype,
     * but never credentials/IP.
     */
    const requestedChannel =
        payload.channel !== undefined
            ? Number(payload.channel)
            : camera.channel;

    const requestedSubtype =
        payload.subtype !== undefined
            ? Number(payload.subtype)
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

/* ============================================================
 * ANSWER
 * ============================================================ */

async function handleAnswer(
    payload
) {

    const sessionId =
        safeString(
            payload.sessionId
        );

    const session =
        sessions.get(sessionId);

    if (!session) {

        console.warn(
            `[ANSWER] session not found: ${sessionId}`
        );

        return;
    }

    if (
        !payload.sdp ||
        !payload.type
    ) {

        throw new Error(
            "Invalid WebRTC answer"
        );
    }

    console.log(
        `[SESSION ${sessionId}] applying browser answer`
    );

    const answer =
        new RTCSessionDescription({
            type: payload.type,
            sdp: payload.sdp
        });

    await session.pc.setRemoteDescription(
        answer
    );

    console.log(
        `[SESSION ${sessionId}] remote description applied`
    );
}

/* ============================================================
 * STOP REQUEST
 * ============================================================ */

function handleStop(
    payload
) {

    const sessionId =
        safeString(
            payload.sessionId
        );

    if (!sessionId) {
        return;
    }

    stopSession(
        sessionId,
        "client-stop"
    );
}

/* ============================================================
 * CAMERA LIST
 * ============================================================ */

function safeCameraForBrowser(
    camera
) {

    /*
     * Never expose:
     *
     * - password
     * - username
     * - LAN IP
     * - RTSP URL
     * - RTSP credentials
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
            Boolean(camera.rtsp),

        onvif:
            Boolean(camera.onvif)
    };
}

function handleCameraListRequest(
    payload
) {

    const sessionId =
        safeString(
            payload.sessionId
        );

    if (!sessionId) {

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

/* ============================================================
 * MQTT MESSAGE HANDLER
 * ============================================================ */

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

        if (sessionId) {

            publishJson(
                topicFor(
                    OFFER_TOPIC_PREFIX,
                    sessionId
                ),
                {
                    sessionId,
                    type: "error",
                    error:
                        error.message ||
                        "WebRTC server error"
                }
            );
        }

        if (sessionId) {

            stopSession(
                sessionId,
                "request-error"
            );
        }
    }
}
/* ============================================================
 * MQTT
 * ============================================================ */

const mqttClient =
    mqtt.connect(
        MQTT_URL,
        {
            username:
                MQTT_USERNAME,

            password:
                MQTT_PASSWORD,

            clientId:
                `smartnode-webrtc-pi-${process.pid}-${Date.now()}`,

            clean: true,

            reconnectPeriod: 3000,

            connectTimeout: 10000,

            keepalive: 30
        }
    );

mqttClient.on(
    "connect",
    () => {

        console.log(
            "================================="
        );

        console.log(
            " MQTT + WEBRTC CAMERA SERVER"
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

                if (error) {

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
    (topic, message) => {

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

/* ============================================================
 * INTERNAL CAMERA VERIFICATION
 * ============================================================ */

function verifyRtsp(
    device,
    username,
    password
) {

    return new Promise(
        resolve => {

            let finished =
                false;

            /*
             * Create a temporary copy of the device
             * containing credentials ONLY in memory.
             *
             * This endpoint is bound to 127.0.0.1.
             */
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
                    success: false,
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

            const finish =
                result => {

                    if (finished) {
                        return;
                    }

                    finished = true;

                    try {
                        ffmpeg.kill(
                            "SIGKILL"
                        );
                    } catch (e) {}

                    resolve(result);
                };

            ffmpeg.stderr.on(
                "data",
                data => {

                    stderr +=
                        data.toString();

                    /*
                     * Avoid unbounded memory.
                     */
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
                        success: false,
                        error:
                            error.message
                    });
                }
            );

            ffmpeg.on(
                "exit",
                (code, signal) => {

                    /*
                     * ffmpeg should normally exit
                     * because of our 2 second duration.
                     *
                     * A successful RTSP decode is
                     * sufficient for registration.
                     */
                    if (
                        code === 0
                    ) {

                        finish({
                            success: true
                        });

                    } else {

                        finish({
                            success: false,
                            error:
                                stderr.trim() ||
                                `FFmpeg exited with code ${code} signal ${signal}`
                        });
                    }
                }
            );

            /*
             * Safety timeout.
             */
            setTimeout(
                () => {

                    finish({
                        success: false,
                        error:
                            "RTSP verification timeout"
                    });

                },
                8000
            );
        }
    );
}

/* ============================================================
 * INTERNAL HTTP SERVER
 * ============================================================ */

const internalServer =
    http.createServer(
        async (
            req,
            res
        ) => {

            /*
             * This server is localhost-only.
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
                        success: true,
                        service:
                            "webrtc-camera-server",
                        mqtt:
                            mqttClient.connected,
                        sessions:
                            sessions.size
                    })
                );

                return;
            }

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
                        success: false,
                        error: "Not found"
                    })
                );

                return;
            }

            try {

                let body = "";

                req.on(
                    "data",
                    chunk => {

                        body +=
                            chunk.toString();

                        /*
                         * Prevent very large body.
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
                                    success: false,
                                    error:
                                        error.message
                                })
                            );
                        }
                    }
                );

            } catch (error) {

                res.writeHead(
                    500,
                    {
                        "Content-Type":
                            "application/json"
                    }
                );

                res.end(
                    JSON.stringify({
                        success: false,
                        error:
                            error.message
                    })
                );
            }
        }
    );

/* ============================================================
 * START INTERNAL SERVER
 * ============================================================ */

internalServer.listen(
    INTERNAL_PORT,
    INTERNAL_HOST,
    () => {

        console.log(
            `[HTTP] Internal server listening on ${INTERNAL_HOST}:${INTERNAL_PORT}`
        );
    }
);
/* ============================================================
 * PROCESS SHUTDOWN
 * ============================================================ */

function shutdown(
    signal
) {

    console.log(
        `[SHUTDOWN] ${signal}`
    );

    /*
     * Stop all WebRTC sessions.
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
     * Close MQTT.
     */
    try {

        mqttClient.end(
            true,
            () => {

                console.log(
                    "[MQTT] Disconnected"
                );

                try {

                    internalServer.close(
                        () => {

                            console.log(
                                "[HTTP] Internal server stopped"
                            );

                            process.exit(0);
                        }
                    );

                } catch (error) {

                    process.exit(0);
                }
            }
        );

    } catch (error) {

        process.exit(0);
    }

    /*
     * Safety exit.
     */
    setTimeout(
        () => {
            process.exit(0);
        },
        3000
    );
}

process.on(
    "SIGINT",
    () => shutdown("SIGINT")
);

process.on(
    "SIGTERM",
    () => shutdown("SIGTERM")
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

console.log(
    "WebRTC camera server starting..."
);