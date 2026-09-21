const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const DATA_DIR = path.join(__dirname, "data");
const CONFIG_FILE = path.join(DATA_DIR, "cameras.enc");

const MASTER_KEY =
    process.env.CAMERA_CONFIG_KEY ||
    "CHANGE_THIS_CAMERA_CONFIG_KEY";

if (MASTER_KEY === "CHANGE_THIS_CAMERA_CONFIG_KEY") {
    console.warn(
        "WARNING: CAMERA_CONFIG_KEY is not configured."
    );
}

/*
 * Convert configured secret into a 32-byte AES key.
 */
function getKey() {
    return crypto
        .createHash("sha256")
        .update(MASTER_KEY)
        .digest();
}

/*
 * Encrypt arbitrary JSON.
 */
function encrypt(data) {
    const iv = crypto.randomBytes(12);
    const key = getKey();

    const cipher = crypto.createCipheriv(
        "aes-256-gcm",
        key,
        iv
    );

    const plaintext =
        Buffer.from(JSON.stringify(data), "utf8");

    const encrypted = Buffer.concat([
        cipher.update(plaintext),
        cipher.final()
    ]);

    const authTag = cipher.getAuthTag();

    return JSON.stringify({
        version: 1,
        iv: iv.toString("base64"),
        authTag: authTag.toString("base64"),
        data: encrypted.toString("base64")
    });
}

/*
 * Decrypt JSON.
 */
function decrypt(value) {
    const parsed =
        typeof value === "string"
            ? JSON.parse(value)
            : value;

    const key = getKey();

    const decipher =
        crypto.createDecipheriv(
            "aes-256-gcm",
            key,
            Buffer.from(parsed.iv, "base64")
        );

    decipher.setAuthTag(
        Buffer.from(parsed.authTag, "base64")
    );

    const decrypted = Buffer.concat([
        decipher.update(
            Buffer.from(parsed.data, "base64")
        ),
        decipher.final()
    ]);

    return JSON.parse(
        decrypted.toString("utf8")
    );
}

/*
 * Read all camera configurations.
 */
function readCameras() {
    if (!fs.existsSync(CONFIG_FILE)) {
        return {};
    }

    try {
        const encrypted =
            fs.readFileSync(
                CONFIG_FILE,
                "utf8"
            );

        return decrypt(encrypted);
    } catch (err) {
        console.error(
            "Unable to decrypt camera configuration:",
            err.message
        );

        throw err;
    }
}

/*
 * Save all camera configurations.
 */
function writeCameras(cameras) {
    fs.mkdirSync(DATA_DIR, {
        recursive: true
    });

    const encrypted =
        encrypt(cameras);

    fs.writeFileSync(
        CONFIG_FILE,
        encrypted,
        {
            encoding: "utf8",
            mode: 0o600
        }
    );

    try {
        fs.chmodSync(
            CONFIG_FILE,
            0o600
        );
    } catch (_) {}
}

/*
 * Generate stable SmartNode device ID.
 */
/*
 * Generate stable SmartNode device ID.
 *
 * For NVR:
 * IP + channel identifies the logical camera channel.
 *
 * For direct IP camera:
 * IP identifies the camera.
 */
function generateDeviceId(device) {
    const channel =
        device.type === "nvr"
            ? Number(device.channel || 1)
            : 0;

    return (
        "cam-" +
        crypto
            .createHash("sha256")
            .update(
                `${device.ip}:${channel}`
            )
            .digest("hex")
            .substring(0, 16)
    );
}


/*
 * Save/update camera.
 */
function saveCamera({
    device,
    username,
    password
}) {
    const cameras =
        readCameras();

    const deviceId =
        generateDeviceId(device);

    cameras[deviceId] = {
        deviceId,

        ip:
            device.ip,

        type:
            device.type ||
            "camera",

        manufacturer:
            device.manufacturer ||
            "",

        model:
            device.model ||
            "",

        /*
         * NVR channel.
         */
        channel:
            Number(device.channel || 1),

        /*
         * Dahua/NVR stream subtype.
         *
         * 0 = main stream
         * 1 = sub stream
         */
        subtype:
            Number(device.subtype ?? 0),

        /*
         * RTSP configuration.
         */
        rtspPort:
            Number(device.rtspPort || 554),

        rtspUrl:
            device.rtspUrl ||
            null,

        rtspPath:
            device.rtspPath ||
            null,

        onvif:
            Boolean(device.onvif),

        rtsp:
            Boolean(device.rtsp),

        xaddrs:
            device.xaddrs || [],

        /*
         * Credentials remain inside the
         * encrypted cameras.enc file.
         */
        credentials: {
            username,
            password
        },

        updatedAt:
            new Date().toISOString()
    };

    writeCameras(cameras);

    /*
     * Never return credentials to the caller.
     */
    return {
        ...cameras[deviceId],
        credentials: undefined
    };
}
/*
 * Get camera configuration.
 */
function getCamera(deviceId) {
    const cameras =
        readCameras();

    return cameras[deviceId] || null;
}

/*
 * List cameras without credentials.
 */
function listCameras() {
    const cameras =
        readCameras();

    return Object.values(cameras)
        .map(camera => ({
            ...camera,
            credentials: undefined
        }));
}

/*
 * Delete camera.
 */
function deleteCamera(deviceId) {
    const cameras =
        readCameras();

    if (!cameras[deviceId]) {
        return false;
    }

    delete cameras[deviceId];

    writeCameras(cameras);

    return true;
}

module.exports = {
    saveCamera,
    getCamera,
    listCameras,
    deleteCamera,
    generateDeviceId
};
