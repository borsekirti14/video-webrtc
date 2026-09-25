require("dotenv").config();
const express = require("express");
const path = require("path");

const {
    scan,
    interfaces
} = require("./network-discovery");

const {
    saveCamera,
    listCameras,
    deleteCamera
} = require("./camera-config");

const app = express();

const PORT =
    Number(process.env.PORT || 8080);

const WEBRTC_INTERNAL_URL =
    process.env.WEBRTC_INTERNAL_URL ||
    "http://127.0.0.1:8090";

app.use(express.json());

/*
 * ============================================================
 * NETWORK INTERFACES
 * ============================================================
 */

app.get("/api/interfaces", (req, res) => {
    res.json({
        interfaces: interfaces()
    });
});

/*
 * ============================================================
 * DISCOVERED DEVICES
 * ============================================================
 */

app.get("/api/devices", async (req, res) => {
    try {
        const devices =
            await scan();

        res.json({
            devices
        });
    } catch (err) {
        console.error(
            "GET /api/devices:",
            err
        );

        res.status(500).json({
            error: err.message
        });
    }
});

/*
 * ============================================================
 * LAN SCAN
 * ============================================================
 */

app.post("/api/scan", async (req, res) => {
    try {
        console.log(
            "\n========== LAN SCAN =========="
        );

        const devices =
            await scan();

        console.log(
            `Scan complete: ${devices.length} device(s)`
        );

        res.json({
            success: true,
            devices
        });

    } catch (err) {

        console.error(
            "POST /api/scan:",
            err
        );

        res.status(500).json({
            success: false,
            error: err.message
        });
    }
});

/*
 * ============================================================
 * REGISTER CAMERA / NVR
 * ============================================================
 *
 * Browser -> Pi
 *
 * {
 *   device,
 *   username,
 *   password
 * }
 *
 * Credentials never go through MQTT.
 */

app.post(
    "/api/cameras/connect",
    async (req, res) => {

        try {

            const {
                device,
                username,
                password
            } = req.body;

            if (!device || !device.ip) {
                return res.status(400).json({
                    success: false,
                    error: "Device IP is required"
                });
            }

            if (
                !username ||
                !String(username).trim()
            ) {
                return res.status(400).json({
                    success: false,
                    error: "Username is required"
                });
            }

            if (
                !password ||
                !String(password)
            ) {
                return res.status(400).json({
                    success: false,
                    error: "Password is required"
                });
            }

            console.log(
                `Camera registration requested: ${device.ip}`
            );

            /*
             * Ask WebRTC server to verify RTSP credentials.
             *
             * Credentials are sent only over localhost.
             */

            const response =
                await fetch(
                    `${WEBRTC_INTERNAL_URL}/internal/camera/verify`,
                    {
                        method: "POST",

                        headers: {
                            "Content-Type":
                                "application/json"
                        },

                        body: JSON.stringify({
                            device,
                            username:
                                String(username),
                            password:
                                String(password)
                        })
                    }
                );

            const result =
                await response.json();

            if (!response.ok || !result.success) {

                return res.status(401).json({
                    success: false,
                    error:
                        result.error ||
                        "Camera authentication failed"
                });
            }

            /*
             * Verification succeeded.
             *
             * Save encrypted credentials.
             */

            const saved =
                saveCamera({
                    device,
                    username:
                        String(username),
                    password:
                        String(password)
                });

            console.log(
                `Camera registered: ${saved.deviceId}`
            );

            res.json({
                success: true,

                camera: saved
            });

        } catch (err) {

            console.error(
                "POST /api/cameras/connect:",
                err.message
            );

            res.status(500).json({
                success: false,
                error: err.message
            });
        }
    }
);

/*
 * ============================================================
 * REGISTERED CAMERAS
 * ============================================================
 */

app.get(
    "/api/cameras",
    (req, res) => {

        try {

            res.json({
                success: true,
                cameras:
                    listCameras()
            });

        } catch (err) {

            res.status(500).json({
                success: false,
                error: err.message
            });
        }
    }
);

/*
 * ============================================================
 * DELETE CAMERA
 * ============================================================
 */

app.delete(
    "/api/cameras/:deviceId",
    (req, res) => {

        try {

            const deleted =
                deleteCamera(
                    req.params.deviceId
                );

            if (!deleted) {

                return res.status(404).json({
                    success: false,
                    error: "Camera not found"
                });
            }

            res.json({
                success: true
            });

        } catch (err) {

            res.status(500).json({
                success: false,
                error: err.message
            });
        }
    }
);

/*
 * ============================================================
 * HEALTH
 * ============================================================
 */

app.get(
    "/health",
    (req, res) => {

        res.json({
            ok: true,
            service:
                "smartnode-camera-phase1"
        });

    }
);

/*
 * ============================================================
 * STATIC FRONTEND
 * ============================================================
 */

app.use(
    express.static(
        path.join(
            __dirname,
            "public"
        )
    )
);

/*
 * ============================================================
 * FRONTEND FALLBACK
 * ============================================================
 */

app.get(
    "/{*splat}",
    (req, res) => {

        res.sendFile(
            path.join(
                __dirname,
                "public",
                "index.html"
            )
        );

    }
);

/*
 * ============================================================
 * ERROR HANDLERS
 * ============================================================
 */

process.on("uncaughtException", (error) => {
    console.error(
        "\n[FATAL] Uncaught Exception:",
        error
    );
    console.error(error.stack);
    // Don't exit - keep server running
});

process.on("unhandledRejection", (reason, promise) => {
    console.error(
        "\n[ERROR] Unhandled Promise Rejection:",
        reason
    );
    // Don't exit - keep server running
});

/*
 * ============================================================
 * START
 * ============================================================
 */

const server = app.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            "================================="
        );

        console.log(
            " SmartNode Camera Gateway"
        );

        console.log(
            "================================="
        );

        console.log(
            `Web UI: http://0.0.0.0:${PORT}`
        );

        console.log(
            `Scan:   POST http://0.0.0.0:${PORT}/api/scan`
        );

        console.log(
            `Camera API: http://0.0.0.0:${PORT}/api/cameras`
        );

        console.log(
            "\n[SERVER] Running... Press Ctrl+C to stop"
        );

    }
);

server.on("error", (error) => {
    console.error(
        "\n[SERVER ERROR]:",
        error
    );
    
    if (error.code === "EADDRINUSE") {
        console.error(
            `Port ${PORT} is already in use. Try a different port.`
        );
    }
    
    process.exit(1);
});
