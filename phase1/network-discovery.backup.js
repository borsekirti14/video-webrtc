#!/usr/bin/env node
/**
 * SmartNode Phase 1 LAN Camera/NVR Discovery
 *
 * Discovery:
 *   - ARP/neighbour table
 *   - Optional concurrent ICMP ping sweep
 *   - TCP service probes
 *   - ONVIF WS-Discovery
 *
 * Phase 1 target: same LAN as the Raspberry Pi.
 */

const os = require("os");
const dgram = require("dgram");
const net = require("net");
const { execFile } = require("child_process");
const { promisify } = require("util");

const execFileAsync = promisify(execFile);

const WS_DISCOVERY_PORT = 3702;
const WS_DISCOVERY_ADDRESS = "239.255.255.250";

const COMMON_PORTS = [
    80, 443, 554, 8000, 8080, 8899, 37777, 34567
];

function getLocalIPv4Interfaces() {
    const result = [];

    for (const [name, entries] of Object.entries(os.networkInterfaces())) {
        for (const e of entries || []) {
            if (e.family === "IPv4" && !e.internal) {
                result.push({
                    interface: name,
                    address: e.address,
                    netmask: e.netmask
                });
            }
        }
    }

    return result;
}

function ipToInt(ip) {
    return ip.split(".").reduce(
        (n, oct) => ((n << 8) | Number(oct)) >>> 0,
        0
    );
}

function intToIp(n) {
    return [
        (n >>> 24) & 255,
        (n >>> 16) & 255,
        (n >>> 8) & 255,
        n & 255
    ].join(".");
}

function cidrRange(ip, netmask) {
    const ipInt = ipToInt(ip);
    const maskInt = ipToInt(netmask);
    const network = (ipInt & maskInt) >>> 0;
    const broadcast = (network | (~maskInt >>> 0)) >>> 0;

    return { network, broadcast };
}

async function readArpTable() {
    const command = process.platform === "win32"
        ? ["arp", ["-a"]]
        : ["ip", ["neigh"]];

    try {
        const { stdout } = await execFileAsync(
            command[0],
            command[1],
            { timeout: 5000 }
        );

        const devices = [];

        for (const line of stdout.split(/\r?\n/)) {
            let m = line.match(
                /(\d+\.\d+\.\d+\.\d+)\s+.*?([0-9a-f]{2}(?::[0-9a-f]{2}){5})/i
            );

            if (!m) {
                m = line.match(
                    /(\d+\.\d+\.\d+\.\d+)\s+([0-9a-f]{2}(?::[0-9a-f]{2}){5})/i
                );
            }

            if (m) {
                devices.push({
                    ip: m[1],
                    mac: m[2].toLowerCase(),
                    source: "arp"
                });
            }
        }

        return devices;
    } catch (err) {
        console.warn("Could not read ARP table:", err.message);
        return [];
    }
}

async function ping(ip) {
    const args = process.platform === "win32"
        ? ["-n", "1", "-w", "500", ip]
        : ["-c", "1", "-W", "1", ip];

    try {
        await execFileAsync("ping", args, { timeout: 1600 });
        return true;
    } catch {
        return false;
    }
}

async function pingSweep(interfaces) {
    const found = new Set();
    const concurrency = 32;

    for (const iface of interfaces) {
        const { network, broadcast } =
            cidrRange(iface.address, iface.netmask);

        const ips = [];

        for (let n = network + 1; n < broadcast; n++) {
            const ip = intToIp(n);
            if (ip !== iface.address) ips.push(ip);
        }

        console.log(
            `Ping sweep ${intToIp(network)} - ${intToIp(broadcast)}`
        );

        for (let i = 0; i < ips.length; i += concurrency) {
            const batch = ips.slice(i, i + concurrency);

            const results = await Promise.all(
                batch.map(async ip => ({
                    ip,
                    alive: await ping(ip)
                }))
            );

            for (const r of results) {
                if (r.alive) found.add(r.ip);
            }
        }
    }

    return [...found];
}

function tcpProbe(ip, port, timeoutMs = 450) {
    return new Promise(resolve => {
        const socket = new net.Socket();
        let finished = false;

        const finish = result => {
            if (finished) return;
            finished = true;
            socket.destroy();
            resolve(result);
        };

        socket.setTimeout(timeoutMs);
        socket.once("connect", () => finish(true));
        socket.once("timeout", () => finish(false));
        socket.once("error", () => finish(false));

        socket.connect(port, ip);
    });
}

async function probeServices(ip) {
    const results = await Promise.all(
        COMMON_PORTS.map(async port => ({
            port,
            open: await tcpProbe(ip, port)
        }))
    );

    return results
        .filter(r => r.open)
        .map(r => r.port);
}

function buildOnvifProbe() {
    return `<?xml version="1.0" encoding="UTF-8"?>
<e:Envelope
 xmlns:e="http://www.w3.org/2003/05/soap-envelope"
 xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing"
 xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery"
 xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
 <e:Header>
  <w:MessageID>uuid:${Date.now()}-${Math.random()}</w:MessageID>
  <w:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To>
  <w:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action>
 </e:Header>
 <e:Body>
  <d:Probe>
   <d:Types>dn:NetworkVideoTransmitter</d:Types>
  </d:Probe>
 </e:Body>
</e:Envelope>`;
}

function onvifDiscovery(interfaces, timeoutMs = 2500) {
    return new Promise(resolve => {
        const found = new Map();
        const sockets = [];
        let remaining = Math.max(1, interfaces.length);

        const finishOne = () => {
            remaining--;
            if (remaining <= 0) {
                resolve([...found.values()]);
            }
        };

        if (!interfaces.length) {
            resolve([]);
            return;
        }

        for (const iface of interfaces) {
            const socket = dgram.createSocket({
                type: "udp4",
                reuseAddr: true
            });

            sockets.push(socket);

            const timer = setTimeout(() => {
                try { socket.close(); } catch {}
                finishOne();
            }, timeoutMs);

            socket.on("error", err => {
                clearTimeout(timer);
                console.warn(
                    `ONVIF ${iface.interface} error:`,
                    err.message
                );
                try { socket.close(); } catch {}
                finishOne();
            });

            socket.on("message", (msg, rinfo) => {
                const text = msg.toString();

                const xaddrsMatch = text.match(
                    /<[^:>]*:?XAddrs[^>]*>([\s\S]*?)<\/[^:>]*:?XAddrs>/i
                );

                const scopesMatch = text.match(
                    /<[^:>]*:?Scopes[^>]*>([\s\S]*?)<\/[^:>]*:?Scopes>/i
                );

                found.set(rinfo.address, {
                    ip: rinfo.address,
                    onvif: true,
                    xaddrs: xaddrsMatch
                        ? xaddrsMatch[1].trim().split(/\s+/)
                        : [],
                    scopes: scopesMatch
                        ? scopesMatch[1].trim().split(/\s+/)
                        : [],
                    source: "onvif"
                });
            });

            socket.bind({ address: iface.address, port: 0 }, () => {
                try {
                    socket.setMulticastInterface(iface.address);
                } catch {}

                try {
                    socket.addMembership(
                        WS_DISCOVERY_ADDRESS,
                        iface.address
                    );
                } catch {}

                const probe = Buffer.from(buildOnvifProbe());

                socket.send(
                    probe,
                    0,
                    probe.length,
                    WS_DISCOVERY_PORT,
                    WS_DISCOVERY_ADDRESS
                );
            });
        }
    });
}

function classify(device) {
    const ports = new Set(device.ports || []);
    const onvif = !!device.onvif;
    const rtsp = ports.has(554);
    const http = ports.has(80) || ports.has(443) || ports.has(8080);
    const dahua = ports.has(37777);
    const hikvision = ports.has(8000);

    if (onvif && rtsp) return "camera/nvr";
    if (onvif) return "onvif-camera/nvr";
    if (rtsp && dahua) return "dahua-camera/nvr";
    if (rtsp && hikvision) return "hikvision-camera/nvr";
    if (rtsp) return "rtsp-device";
    if (http) return "http-device";
    return "unknown";
}

async function scan() {
    const interfaces = getLocalIPv4Interfaces();

    if (!interfaces.length) {
        throw new Error("No active IPv4 LAN interface found");
    }

    console.log("\nInterfaces:");
    for (const iface of interfaces) {
        console.log(
            `  ${iface.interface}: ${iface.address} / ${iface.netmask}`
        );
    }

    const devices = new Map();

    // ARP/neighbour table
    for (const d of await readArpTable()) {
        devices.set(d.ip, { ...d });
    }

    // Optional full LAN ping sweep.
    if (process.env.SMARTNODE_PING_SWEEP === "1") {
        for (const ip of await pingSweep(interfaces)) {
            devices.set(ip, {
                ...(devices.get(ip) || {}),
                ip,
                source: devices.has(ip)
                    ? devices.get(ip).source
                    : "ping"
            });
        }
    }

    // ONVIF WS-Discovery
    for (const d of await onvifDiscovery(interfaces)) {
        devices.set(d.ip, {
            ...(devices.get(d.ip) || {}),
            ...d
        });
    }

    console.log(
        `Checking services on ${devices.size} discovered device(s)...`
    );

    // Service probes in parallel for the discovered hosts.
    const hostResults = await Promise.all(
        [...devices.values()].map(async device => ({
            ...device,
            ports: await probeServices(device.ip)
        }))
    );

    for (const device of hostResults) {
        device.rtsp = device.ports.includes(554);
        device.onvif = !!device.onvif;
        device.type = classify(device);
    }

    return hostResults
        .sort((a, b) =>
            a.ip.localeCompare(b.ip, undefined, { numeric: true })
        )
        .map(d => ({
            ip: d.ip,
            mac: d.mac || null,
            type: d.type,
            onvif: d.onvif,
            rtsp: d.rtsp,
            ports: d.ports || [],
            xaddrs: d.xaddrs || [],
            source: d.source || null
        }));
}

if (require.main === module) {
    scan()
        .then(result => {
            console.log("\nDISCOVERED DEVICES");
            console.table(result);
            console.log("\nJSON:");
            console.log(JSON.stringify(result, null, 2));
        })
        .catch(err => {
            console.error("Discovery failed:", err);
            process.exit(1);
        });
}

module.exports = {
    scan,
    interfaces: getLocalIPv4Interfaces
};
