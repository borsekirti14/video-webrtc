require("dotenv").config();

/**
 * ============================================================
 * SmartNode Phase 1 Camera/NVR WebRTC Server
 * ============================================================
 *
 * Architecture:
 *
 * Browser
 *    |
 *    | MQTT start { sessionId, deviceId, channel, subtype }
 *    v
 * MQTT Broker
 *    |
 *    v
 * Raspberry Pi
 *    |
 *    +--> camera-config.js
 *    |       |
 *    |       +--> encrypted cameras.enc
 *    |
 *    +--> RTSP / FFmpeg
 *    |
 *    +--> WebRTC
 *            |
 *            v
 *         Browser
 *
 * Camera credentials NEVER travel through MQTT.
 *
 * Registration:
 *
 * server.js
 *    |
 *    +--> POST /internal/camera/verify
 *              |
 *              +--> FFmpeg RTSP test
 *
 * ============================================================
 */

"use strict";

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
    getCamera
} = require("./camera-config");

/*
 * ============================================================
 * Configuration
 * ============================================================
 */

const MQTT_URL =
    process.env.MQTT_URL ||
    "tcp://connect.smartnode.in:1883";

const MQTT_USERNAME =
    process.env.MQTT_USERNAME;

const MQTT_PASSWORD =
    process.env.MQTT_PASSWORD;

const INTERNAL_HOST =
    process.env.WEBRTC_INTERNAL_HOST ||
    "127.0.0.1";

const INTERNAL_PORT =
    Number(
        process.env.WEBRTC_INTERNAL_PORT ||
        8090
    );

const REQUEST_TOPIC =
    "smartnode/test/webrtc/request";

const OFFER_TOPIC_PREFIX =
    "smartnode/test/webrtc/offer";

const ANSWER_TOPIC_PREFIX =
    "smartnode/test/webrtc/answer";

/*
 * Video output expected by wrtc RTCVideoSource.
 *
 * We use I420 / YUV420p.
 */

const WIDTH = 640;
const HEIGHT = 360;
const FPS = 30;

const FRAME_SIZE =
    WIDTH * HEIGHT +
    (WIDTH / 2) * (HEIGHT / 2) +
    (WIDTH / 2) * (HEIGHT / 2);

/*
 * ICE servers.
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

/*
 * Each browser tab gets its own session.
 *
 * sessionId -> session object
 */

const sessions = new Map();


/*
 * ============================================================
 * Utility
 * ============================================================
 */

function topicFor(prefix, sessionId) {
    return `${prefix}/${sessionId}`;
}


function safeString(value) {
    return value === undefined ||
        value === null
        ? ""
        : String(value);
}


/*
 * ============================================================
 * RTSP URL
 * ============================================================
 *
 * Credentials can come from:
 *
 * 1. credentials argument during registration verification
 * 2. camera.credentials during normal Live View
 *
 * IMPORTANT:
 * Never log the returned URL.
 * ============================================================
 */

function buildRtspUrl(
    device,
    credentials = {}
) {
    if (!device || !device.ip) {
        throw new Error(
            "Device IP is required"
        );
    }

    const username =
        credentials.username ||
        device.credentials?.username;

    const password =
        credentials.password ||
        device.credentials?.password;

    if (!username || !password) {
        throw new Error(
            `Credentials not available for ${device.ip}`
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
     * --------------------------------------------------------
     * Custom complete RTSP URL
     * --------------------------------------------------------
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

    /*
     * --------------------------------------------------------
     * Dahua / NVR
     * --------------------------------------------------------
     *
     * Example:
     *
     * /cam/realmonitor?channel=1&subtype=0
     *
     * subtype:
     *
     * 0 = main stream
     * 1 = sub stream
     * --------------------------------------------------------
     */

    if (
        type.includes("dahua") ||
        type.includes("nvr") ||
        device.ports?.includes?.(37777)||
    device.rtsp === true
    ) {
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

        return (
            `rtsp://${user}:${pass}` +
            `@${device.ip}:${port}` +
            `/cam/realmonitor` +
            `?channel=${channel}` +
            `&subtype=${subtype}`
        );
    }

    /*
     * --------------------------------------------------------
     * Generic RTSP camera
     * --------------------------------------------------------
     */

    if (device.rtspPath) {
        const port =
            Number(
                device.rtspPort || 554
            );

        let path =
            String(device.rtspPath);

        if (!path.startsWith("/")) {
            path = "/" + path;
        }

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


/*
 * ============================================================
 * MQTT publish helper
 * ============================================================
 */

function publish(
    topic,
    payload
) {
    return new Promise(
        (resolve, reject) => {

            client.publish(
                topic,
                JSON.stringify(payload),
                {
                    qos: 0,
                    retain: false
                },
                err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        }
    );
}


/*
 * ============================================================
 * ICE gathering
 * ============================================================
 */

function waitForIceComplete(
    pc,
    timeoutMs = 10000
) {
    if (
        pc.iceGatheringState ===
        "complete"
    ) {
        return Promise.resolve();
    }

    return new Promise(
        resolve => {

            let finished = false;

            const timer =
                setTimeout(
                    finish,
                    timeoutMs
                );

            function finish() {
                if (finished) {
                    return;
                }

                finished = true;

                clearTimeout(timer);

                try {
                    pc.removeEventListener(
                        "icegatheringstatechange",
                        check
                    );
                } catch {}

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

            pc.addEventListener(
                "icegatheringstatechange",
                check
            );

            check();
        }
    );
}


/*
 * ============================================================
 * Stop session
 * ============================================================
 */

function stopSession(
    sessionId
) {
    if (!sessionId) {
        return;
    }

    const session =
        sessions.get(sessionId);

    if (!session) {
        return;
    }

    console.log(
        `Stopping session ${sessionId}`
    );

    /*
     * Stop FFmpeg.
     */

    if (session.ffmpeg) {
        try {
            session.ffmpeg.kill(
                "SIGTERM"
            );
        } catch {}

        session.ffmpeg = null;
    }

    /*
     * Stop video track.
     */

    if (session.videoTrack) {
        try {
            session.videoTrack.stop();
        } catch {}
    }

    /*
     * Close PeerConnection.
     */

    if (session.pc) {
        try {
            session.pc.close();
        } catch {}
    }

    session.frameBuffer =
        Buffer.alloc(0);

    sessions.delete(sessionId);
}


/*
 * ============================================================
 * Start RTSP / FFmpeg
 * ============================================================
 */

function startRTSP(
    session,
    rtspUrl
) {
    if (session.ffmpeg) {
        return;
    }

    console.log(
        `Starting FFmpeg for ${session.id}`
    );

    /*
     * NEVER log rtspUrl.
     *
     * It contains camera credentials.
     */

    const ffmpeg =
        spawn(
            "ffmpeg",
            [
                "-hide_banner",

                "-loglevel",
                "warning",

                "-rtsp_transport",
                "tcp",

                "-i",
                rtspUrl,

                /*
                 * No audio for Phase 1.
                 */

                "-an",

                /*
                 * Convert incoming video to
                 * browser-friendly resolution.
                 */

                "-vf",
                `scale=${WIDTH}:${HEIGHT}:force_original_aspect_ratio=decrease,pad=${WIDTH}:${HEIGHT}:(ow-iw)/2:(oh-ih)/2`,

                /*
                 * Normalize FPS.
                 */

                "-r",
                String(FPS),

                /*
                 * RTCVideoSource expects I420.
                 */

                "-pix_fmt",
                "yuv420p",

                /*
                 * Raw frames.
                 */

                "-f",
                "rawvideo",

                "pipe:1"
            ],
            {
                stdio: [
                    "ignore",
                    "pipe",
                    "pipe"
                ]
            }
        );

    session.ffmpeg =
        ffmpeg;

    session.frameBuffer =
        Buffer.alloc(0);

    session.frameCount = 0;

    session.lastFpsLog =
        Date.now();


    /*
     * --------------------------------------------------------
     * Raw video frames
     * --------------------------------------------------------
     */

    ffmpeg.stdout.on(
        "data",
        chunk => {

            if (!sessions.has(session.id)) {
                return;
            }

            session.frameBuffer =
                Buffer.concat([
                    session.frameBuffer,
                    chunk
                ]);

            while (
                session.frameBuffer.length >=
                FRAME_SIZE
            ) {

                const frame =
                    session.frameBuffer.subarray(
                        0,
                        FRAME_SIZE
                    );

                session.frameBuffer =
                    session.frameBuffer.subarray(
                        FRAME_SIZE
                    );

                try {

                    session.videoSource.onFrame({
                        width: WIDTH,
                        height: HEIGHT,
                        data: Buffer.from(frame)
                    });

                    session.frameCount++;

                    const now =
                        Date.now();

                    if (
                        now -
                        session.lastFpsLog >=
                        1000
                    ) {

                        console.log(
                            `${session.id}: ${session.frameCount} frames/sec`
                        );

                        session.frameCount = 0;

                        session.lastFpsLog =
                            now;
                    }

                } catch (err) {

                    console.error(
                        `${session.id}: RTCVideoSource error:`,
                        err.message
                    );
                }
            }
        }
    );


    /*
     * --------------------------------------------------------
     * FFmpeg stderr
     * --------------------------------------------------------
     */

    ffmpeg.stderr.on(
        "data",
        data => {

            const message =
                data
                    .toString()
                    .trim();

            if (!message) {
                return;
            }

            console.error(
                `${session.id}: FFmpeg: ${message}`
            );
        }
    );


    /*
     * --------------------------------------------------------
     * FFmpeg process error
     * --------------------------------------------------------
     */

    ffmpeg.on(
        "error",
        err => {

            console.error(
                `${session.id}: FFmpeg process error:`,
                err.message
            );
        }
    );


    /*
     * --------------------------------------------------------
     * FFmpeg exit
     * --------------------------------------------------------
     */

    ffmpeg.on(
        "exit",
        (code, signal) => {

            console.log(
                `${session.id}: FFmpeg exited code=${code} signal=${signal}`
            );

            if (
                sessions.has(
                    session.id
                )
            ) {
                session.ffmpeg =
                    null;
            }
        }
    );
}


/*
 * ============================================================
 * WebRTC
 * ============================================================
 */

async function startWebRTC(
    session
) {
    /*
     * Video source.
     */

    session.videoSource =
        new RTCVideoSource();

    /*
     * Video track.
     */

    session.videoTrack =
        session.videoSource
            .createTrack();

    /*
     * PeerConnection.
     */

    session.pc =
        new RTCPeerConnection({
            iceServers: ICE_SERVERS,
            iceCandidatePoolSize: 10
        });

    /*
     * Send video only.
     */

    session.pc.addTransceiver(
        session.videoTrack,
        {
            direction: "sendonly"
        }
    );


    /*
     * ICE state.
     */

    session.pc.oniceconnectionstatechange =
        () => {

            console.log(
                `${session.id}: ICE ${session.pc.iceConnectionState}`
            );
        };


    /*
     * WebRTC connection state.
     */

    session.pc.onconnectionstatechange =
        () => {

            console.log(
                `${session.id}: WebRTC ${session.pc.connectionState}`
            );

            if (
                [
                    "failed",
                    "closed"
                ].includes(
                    session.pc.connectionState
                )
            ) {

                setTimeout(
                    () => {
                        stopSession(
                            session.id
                        );
                    },
                    100
                );
            }
        };


    /*
     * Create offer.
     */

    const offer =
        await session.pc
            .createOffer();


    /*
     * Set local description.
     */

    await session.pc
        .setLocalDescription(
            offer
        );


    /*
     * Wait until ICE gathering finishes.
     */

    await waitForIceComplete(
        session.pc
    );


    /*
     * Publish offer to:
     *
     * smartnode/test/webrtc/offer/<sessionId>
     */

    await publish(
        topicFor(
            OFFER_TOPIC_PREFIX,
            session.id
        ),
        {
            sessionId: session.id,
            type: "offer",
            sdp:
                session.pc
                    .localDescription
                    .sdp
        }
    );

    console.log(
        `Offer published for ${session.id}`
    );
}


/*
 * ============================================================
 * START CAMERA SESSION
 * ============================================================
 *
 * MQTT payload:
 *
 * {
 *   "sessionId": "...",
 *   "action": "start",
 *   "deviceId": "...",
 *   "channel": 1,
 *   "subtype": 0
 * }
 *
 * NO USERNAME
 * NO PASSWORD
 * ============================================================
 */

async function handleStart(
    payload
) {
    const sessionId =
        safeString(
            payload.sessionId
        );

    if (!sessionId) {
        throw new Error(
            "start request has no sessionId"
        );
    }

    const deviceId =
        safeString(
            payload.deviceId
        );

    if (!deviceId) {
        throw new Error(
            "start request has no deviceId"
        );
    }


    /*
     * --------------------------------------------------------
     * Load camera from encrypted configuration.
     * --------------------------------------------------------
     */

    const camera =
        getCamera(deviceId);

    if (!camera) {
        throw new Error(
            `Camera not found: ${deviceId}`
        );
    }


    /*
     * Optional channel override.
     */

    if (
        payload.channel !==
        undefined
    ) {
        camera.channel =
            Number(
                payload.channel
            );
    }


    /*
     * Optional subtype override.
     */

    if (
        payload.subtype !==
        undefined
    ) {
        camera.subtype =
            Number(
                payload.subtype
            );
    }


    /*
     * Build RTSP URL using credentials
     * loaded from encrypted configuration.
     */

    const rtspUrl =
        buildRtspUrl(
            camera
        );


    /*
     * If this session already exists,
     * clean it first.
     */

    stopSession(
        sessionId
    );


    /*
     * Create isolated session.
     */

    const session = {
        id: sessionId,

        deviceId,

        device: {
            ip: camera.ip,
            type: camera.type,
            manufacturer:
                camera.manufacturer,
            model: camera.model,
            channel:
                camera.channel,
            subtype:
                camera.subtype,
            rtspPort:
                camera.rtspPort
        },

        /*
         * RTSP URL is kept only in memory.
         *
         * It is never sent through MQTT.
         */

        rtspUrl,

        pc: null,

        videoSource: null,

        videoTrack: null,

        ffmpeg: null,

        frameBuffer:
            Buffer.alloc(0),

        frameCount: 0,

        lastFpsLog:
            Date.now(),

        answerReceived: false
    };


    sessions.set(
        sessionId,
        session
    );


    console.log(
        `\nSTART ${sessionId}:`
    );

    console.log(
        JSON.stringify({
            deviceId,
            ip: camera.ip,
            type: camera.type,
            channel:
                camera.channel,
            subtype:
                camera.subtype
        })
    );


    try {

        /*
         * First create WebRTC offer.
         */

        await startWebRTC(
            session
        );


        /*
         * Start RTSP after offer
         * has been published.
         */

        startRTSP(
            session,
            rtspUrl
        );

    } catch (err) {

        stopSession(
            sessionId
        );

        throw err;
    }
}


/*
 * ============================================================
 * WEBRTC ANSWER
 * ============================================================
 */

async function handleAnswer(
    payload
) {
    const sessionId =
        safeString(
            payload.sessionId
        );

    if (!sessionId) {
        return;
    }

    const session =
        sessions.get(
            sessionId
        );

    if (!session) {

        console.warn(
            `Answer for unknown session ${sessionId}`
        );

        return;
    }


    if (
        session.answerReceived ||
        !session.pc
    ) {
        return;
    }


    if (!payload.sdp) {
        throw new Error(
            "answer has no SDP"
        );
    }


    session.answerReceived =
        true;


    await session.pc
        .setRemoteDescription(
            new RTCSessionDescription({
                type: "answer",
                sdp: payload.sdp
            })
        );


    console.log(
        `Answer accepted for ${sessionId}`
    );
}


/*
 * ============================================================
 * RTSP VERIFICATION
 * ============================================================
 *
 * Used by:
 *
 * POST /api/cameras/connect
 *
 * This is deliberately separate from
 * normal Live View.
 * ============================================================
 */

function verifyRTSP(
    device,
    credentials
) {
    return new Promise(
        resolve => {

            let finished = false;

            let ffmpeg = null;


            function finish(
                result
            ) {
                if (finished) {
                    return;
                }

                finished = true;

                if (ffmpeg) {
                    try {
                        ffmpeg.kill(
                            "SIGTERM"
                        );
                    } catch {}
                }

                resolve(result);
            }


            try {

                /*
                 * Build URL using credentials
                 * supplied by registration.
                 */

                const rtspUrl =
                    buildRtspUrl(
                        device,
                        credentials
                    );


                console.log(
                    `Testing RTSP: ${device.ip} ` +
                    `channel=${device.channel || 1} ` +
                    `subtype=${device.subtype ?? 0}`
                );


                /*
                 * IMPORTANT:
                 * Do not log rtspUrl.
                 */

                ffmpeg =
                    spawn(
                        "ffmpeg",
                        [
                            "-hide_banner",

                            "-loglevel",
                            "error",

                            "-rtsp_transport",
                            "tcp",

                            "-i",
                            rtspUrl,

                            /*
                             * We only need to verify
                             * that FFmpeg can decode
                             * the stream.
                             */

                            "-t",
                            "2",

                            "-f",
                            "null",

                            "-"
                        ],
                        {
                            stdio: [
                                "ignore",
                                "ignore",
                                "pipe"
                            ]
                        }
                    );


                let stderr = "";


                ffmpeg.stderr.on(
                    "data",
                    data => {

                        stderr +=
                            data.toString();

                        /*
                         * Avoid unlimited stderr.
                         */

                        if (
                            stderr.length >
                            10000
                        ) {
                            stderr =
                                stderr.slice(
                                    -10000
                                );
                        }
                    }
                );


                ffmpeg.on(
                    "error",
                    err => {

                        finish({
                            success: false,
                            error:
                                `FFmpeg error: ${err.message}`
                        });
                    }
                );


                ffmpeg.on(
                    "close",
                    code => {

                        if (
                            code === 0
                        ) {

                            finish({
                                success: true
                            });

                            return;
                        }


                        /*
                         * Do not expose credentials
                         * in the error response.
                         */

                        const cleanError =
                            stderr
                                .replace(
                                    /rtsp:\/\/[^\s]+/gi,
                                    "RTSP_URL"
                                )
                                .trim();


                        finish({
                            success: false,
                            error:
                                cleanError ||
                                "RTSP connection failed"
                        });
                    }
                );


            } catch (err) {

                finish({
                    success: false,
                    error: err.message
                });
            }
        }
    );
}


/*
 * ============================================================
 * INTERNAL HTTP SERVER
 * ============================================================
 *
 * server.js calls:
 *
 * POST http://127.0.0.1:8090/internal/camera/verify
 *
 * Credentials are accepted ONLY from localhost.
 * ============================================================
 */

function startInternalServer() {

    const server =
        http.createServer(
            (req, res) => {

                /*
                 * Only localhost is allowed.
                 */

                const remote =
                    req.socket.remoteAddress;

                const isLocal =
                    remote ===
                        "127.0.0.1" ||
                    remote ===
                        "::1" ||
                    remote ===
                        "::ffff:127.0.0.1";


                if (!isLocal) {

                    res.writeHead(
                        403,
                        {
                            "Content-Type":
                                "application/json"
                        }
                    );

                    return res.end(
                        JSON.stringify({
                            success: false,
                            error: "Forbidden"
                        })
                    );
                }


                /*
                 * Only POST endpoint.
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

                    return res.end(
                        JSON.stringify({
                            success: false,
                            error: "Not found"
                        })
                    );
                }


                let body = "";


                req.on(
                    "data",
                    chunk => {

                        body +=
                            chunk.toString();

                        /*
                         * Maximum request size:
                         * 1 MB
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


                            const {
                                device,
                                username,
                                password
                            } = payload;


                            if (
                                !device ||
                                !device.ip ||
                                !username ||
                                !password
                            ) {

                                res.writeHead(
                                    400,
                                    {
                                        "Content-Type":
                                            "application/json"
                                    }
                                );

                                return res.end(
                                    JSON.stringify({
                                        success: false,
                                        error:
                                            "Device, username and password are required"
                                    })
                                );
                            }


                            /*
                             * Verify RTSP.
                             */

                            const result =
                                await verifyRTSP(
                                    device,
                                    {
                                        username:
                                            String(
                                                username
                                            ),

                                        password:
                                            String(
                                                password
                                            )
                                    }
                                );


                            if (
                                result.success
                            ) {

                                res.writeHead(
                                    200,
                                    {
                                        "Content-Type":
                                            "application/json"
                                    }
                                );

                                return res.end(
                                    JSON.stringify({
                                        success: true
                                    })
                                );
                            }


                            res.writeHead(
                                401,
                                {
                                    "Content-Type":
                                        "application/json"
                                }
                            );

                            return res.end(
                                JSON.stringify({
                                    success: false,
                                    error:
                                        result.error ||
                                        "RTSP verification failed"
                                })
                            );


                        } catch (err) {

                            console.error(
                                "Camera verification error:",
                                err.message
                            );


                            res.writeHead(
                                400,
                                {
                                    "Content-Type":
                                        "application/json"
                                }
                            );

                            return res.end(
                                JSON.stringify({
                                    success: false,
                                    error:
                                        err.message
                                })
                            );
                        }
                    }
                );
            }
        );


    server.listen(
        INTERNAL_PORT,
        INTERNAL_HOST,
        () => {

            console.log(
                `Internal camera API listening on ` +
                `http://${INTERNAL_HOST}:${INTERNAL_PORT}`
            );
        }
    );


    server.on(
        "error",
        err => {

            console.error(
                "Internal HTTP server error:",
                err.message
            );
        }
    );


    return server;
}


/*
 * ============================================================
 * MQTT
 * ============================================================
 */

if (
    !MQTT_USERNAME ||
    !MQTT_PASSWORD
) {

    console.warn(
        "WARNING: MQTT_USERNAME/MQTT_PASSWORD " +
        "are not configured in environment."
    );
}


const client =
    mqtt.connect(
        MQTT_URL,
        {
            clientId:
                `camera-pi-${Date.now()}-` +
                Math.random()
                    .toString(16)
                    .slice(2, 8),

            username:
                MQTT_USERNAME,

            password:
                MQTT_PASSWORD,

            reconnectPeriod:
                2000,

            clean: true,

            keepalive: 30
        }
    );


/*
 * ============================================================
 * MQTT CONNECT
 * ============================================================
 */

client.on(
    "connect",
    () => {

        console.log(
            "================================="
        );

        console.log(
            " SmartNode Camera WebRTC Server"
        );

        console.log(
            "================================="
        );

        console.log(
            "MQTT connected"
        );


        client.subscribe(
            [
                REQUEST_TOPIC,
                `${ANSWER_TOPIC_PREFIX}/+`
            ],
            {
                qos: 0
            },
            err => {

                if (err) {

                    console.error(
                        "MQTT subscribe error:",
                        err.message
                    );

                    return;
                }


                console.log(
                    `Subscribed: ${REQUEST_TOPIC}`
                );

                console.log(
                    `Subscribed: ${ANSWER_TOPIC_PREFIX}/+`
                );
            }
        );
    }
);


/*
 * ============================================================
 * MQTT RECONNECT
 * ============================================================
 */

client.on(
    "reconnect",
    () => {

        console.log(
            "MQTT reconnecting..."
        );
    }
);


/*
 * ============================================================
 * MQTT ERROR
 * ============================================================
 */

client.on(
    "error",
    err => {

        console.error(
            "MQTT error:",
            err.message
        );
    }
);


/*
 * ============================================================
 * MQTT MESSAGE
 * ============================================================
 */

client.on(
    "message",
    async (
        topic,
        message
    ) => {

        try {

            const payload =
                JSON.parse(
                    message.toString()
                );


            /*
             * ------------------------------------------------
             * START / STOP
             * ------------------------------------------------
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

                    stopSession(
                        safeString(
                            payload.sessionId
                        )
                    );
                }


                return;
            }


            /*
             * ------------------------------------------------
             * ANSWER
             * ------------------------------------------------
             */

            if (
                topic.startsWith(
                    `${ANSWER_TOPIC_PREFIX}/`
                )
            ) {

                await handleAnswer(
                    payload
                );
            }


        } catch (err) {

            console.error(
                "MQTT message handling error:",
                err.message
            );
        }
    }
);


/*
 * ============================================================
 * START INTERNAL SERVER
 * ============================================================
 */

const internalServer =
    startInternalServer();


/*
 * ============================================================
 * SHUTDOWN
 * ============================================================
 */

function shutdown(
    signal
) {

    console.log(
        `\nReceived ${signal}. Shutting down...`
    );


    /*
     * Stop all camera sessions.
     */

    for (
        const sessionId of
        sessions.keys()
    ) {

        stopSession(
            sessionId
        );
    }


    /*
     * Close MQTT.
     */

    try {

        client.end(
            true,
            () => {

                try {
                    internalServer.close();
                } catch {}

                process.exit(0);
            }
        );

    } catch {

        process.exit(0);
    }
}


process.on(
    "SIGINT",
    () => shutdown("SIGINT")
);

process.on(
    "SIGTERM",
    () => shutdown("SIGTERM")
);