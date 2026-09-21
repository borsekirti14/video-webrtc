/**
 * SmartNode LAN Camera/NVR Discovery
 *
 * Best-effort classification:
 *   - NVR: recording/replay capability or strong NVR model/scope indicator
 *   - camera: video/media capability without recording/replay
 *   - onvif-device: ONVIF found but type cannot be determined confidently
 *
 * Environment:
 *   SMARTNODE_PING_SWEEP=1
 *   CAMERA_USERNAME=<ONVIF/vendor username>
 *   CAMERA_PASSWORD=<ONVIF/vendor password>
 */

const os = require("os");
const dgram = require("dgram");
const http = require("http");
const https = require("https");
const net = require("net");
const { execFile } = require("child_process");

const COMMON_PORTS = [80, 443, 554, 8000, 8080, 8899, 37777, 34567];
const ONVIF_UDP_PORT = 3702;
const DISCOVERY_TIMEOUT = 2500;
const HTTP_TIMEOUT = 2500;

function interfaces() {
  const result = [];
  for (const [name, list] of Object.entries(os.networkInterfaces())) {
    for (const item of list || []) {
      if (item.family === "IPv4" && !item.internal) {
        result.push({ interface: name, address: item.address, netmask: item.netmask });
      }
    }
  }
  return result;
}

function ipToInt(ip) {
  return ip.split(".").reduce((n, x) => ((n << 8) + Number(x)) >>> 0, 0);
}
function intToIp(n) {
  return [24,16,8,0].map(s => (n >>> s) & 255).join(".");
}
function cidrHosts(address, netmask) {
  const ip = ipToInt(address), mask = ipToInt(netmask);
  const network = (ip & mask) >>> 0;
  const broadcast = (network | (~mask >>> 0)) >>> 0;
  const count = broadcast - network - 1;
  if (count <= 0 || count > 65534) return [];
  const hosts = [];
  for (let i = network + 1; i < broadcast; i++) hosts.push(intToIp(i >>> 0));
  return hosts;
}

function exec(cmd, args) {
  return new Promise(resolve => {
    execFile(cmd, args, { timeout: 2500 }, (err, stdout) =>
      resolve(err ? "" : String(stdout || ""))
    );
  });
}

async function arpNeighbors() {
  const output = await exec("ip", ["neigh"]);
  const devices = [];
  for (const line of output.split(/\n/)) {
    const m = line.match(/^(\d+\.\d+\.\d+\.\d+)\s+dev\s+(\S+).*?(?:lladdr\s+([0-9a-f:]{17}))?.*?(REACHABLE|STALE|DELAY|PROBE|PERMANENT|FAILED)?/i);
    if (m && m[3] && m[3] !== "00:00:00:00:00:00") {
      devices.push({ ip: m[1], interface: m[2], mac: m[3].toLowerCase(), source: "arp" });
    }
  }
  return devices;
}

function tcpOpen(ip, port, timeout = 700) {
  return new Promise(resolve => {
    const s = new net.Socket();
    let done = false;
    const finish = ok => {
      if (done) return;
      done = true;
      try { s.destroy(); } catch {}
      resolve(ok);
    };
    s.setTimeout(timeout);
    s.once("connect", () => finish(true));
    s.once("timeout", () => finish(false));
    s.once("error", () => finish(false));
    s.connect(port, ip);
  });
}

async function probePorts(ip) {
  const checks = await Promise.all(COMMON_PORTS.map(async p => [p, await tcpOpen(ip, p)]));
  return checks.filter(x => x[1]).map(x => x[0]);
}

function xmlTag(xml, tag) {
  const re = new RegExp(`<[^>]*:?${tag}\\b[^>]*>([\\s\\S]*?)</[^>]*:?${tag}>`, "i");
  const m = xml.match(re);
  return m ? m[1].trim() : "";
}

function allTags(xml, tag) {
  const re = new RegExp(`<[^>]*:?${tag}\\b[^>]*>([\\s\\S]*?)</[^>]*:?${tag}>`, "gi");
  return [...xml.matchAll(re)].map(m => m[1].trim()).filter(Boolean);
}

function hasToken(xml, token) {
  return new RegExp(token, "i").test(xml || "");
}

function httpRequest(urlString, body = null, headers = {}) {
  return new Promise(resolve => {
    let u;
    try { u = new URL(urlString); } catch { return resolve({ ok:false, status:0, body:"" }); }
    const lib = u.protocol === "https:" ? https : http;
    const req = lib.request({
      hostname: u.hostname,
      port: u.port || (u.protocol === "https:" ? 443 : 80),
      path: u.pathname + u.search,
      method: body === null ? "GET" : "POST",
      rejectUnauthorized: false,
      timeout: HTTP_TIMEOUT,
      headers: {
        "User-Agent": "SmartNode-Discovery/1.0",
        ...(body !== null ? {
          "Content-Type": "application/soap+xml; charset=utf-8",
          "Content-Length": Buffer.byteLength(body)
        } : {}),
        ...headers
      }
    }, res => {
      let data = "";
      res.setEncoding("utf8");
      res.on("data", c => data += c);
      res.on("end", () => resolve({ ok: res.statusCode >= 200 && res.statusCode < 500, status: res.statusCode || 0, body:data }));
    });
    req.on("error", () => resolve({ ok:false, status:0, body:"" }));
    req.on("timeout", () => { try { req.destroy(); } catch {} resolve({ ok:false, status:0, body:"" }); });
    if (body !== null) req.write(body);
    req.end();
  });
}

function basicAuth() {
  const u = process.env.CAMERA_USERNAME;
  const p = process.env.CAMERA_PASSWORD;
  return u ? "Basic " + Buffer.from(`${u}:${p || ""}`).toString("base64") : null;
}

const SOAP_GET_DEVICE_INFO = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">
<s:Body><tds:GetDeviceInformation xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/></s:Body>
</s:Envelope>`;

const SOAP_GET_CAPS = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">
<s:Body><tds:GetCapabilities xmlns:tds="http://www.onvif.org/ver10/device/wsdl"><tds:Category>All</tds:Category></tds:GetCapabilities></s:Body>
</s:Envelope>`;

async function onvifDetails(xaddr, ip) {
  const result = {
    manufacturer: "",
    model: "",
    firmware: "",
    hardwareId: "",
    scopes: [],
    capabilities: [],
    recording: false,
    replay: false,
    media: false,
    deviceInfoAuthenticated: false
  };

  if (!xaddr) return result;

  const auth = basicAuth();
  const headers = auth ? { Authorization: auth } : {};

  let r = await httpRequest(xaddr, SOAP_GET_DEVICE_INFO, headers);
  if (r.body) {
    result.manufacturer = xmlTag(r.body, "Manufacturer");
    result.model = xmlTag(r.body, "Model");
    result.firmware = xmlTag(r.body, "FirmwareVersion");
    result.hardwareId = xmlTag(r.body, "HardwareId");
    result.deviceInfoAuthenticated = !!(result.manufacturer || result.model);
  }

  r = await httpRequest(xaddr, SOAP_GET_CAPS, headers);
  if (r.body) {
    result.media = hasToken(r.body, "Media|Media2");
    result.recording = hasToken(r.body, "Recording");
    result.replay = hasToken(r.body, "Replay");
    if (hasToken(r.body, "Event")) result.capabilities.push("event");
    if (hasToken(r.body, "PTZ")) result.capabilities.push("ptz");
    if (result.media) result.capabilities.push("media");
    if (result.recording) result.capabilities.push("recording");
    if (result.replay) result.capabilities.push("replay");
  }

  return result;
}

function vendorClassification(d) {
  const s = `${d.manufacturer} ${d.model} ${d.scopes.join(" ")}`.toLowerCase();
  const nvrStrong = /\b(nvr|dvr|xvr|network video recorder|digital video recorder)\b/i.test(s);
  const cameraStrong = /\b(ipc|camera|bullet|dome|turret|ptz camera|network camera)\b/i.test(s);

  if (d.recording || d.replay || nvrStrong) {
    return { type: "nvr", confidence: "high" };
  }
  if (cameraStrong) {
    return { type: "camera", confidence: "high" };
  }
  if (d.media) {
    return { type: "camera", confidence: "medium" };
  }
  return { type: "onvif-device", confidence: "low" };
}

function onvifProbe(ip, ifaceAddress) {
  return new Promise(resolve => {
    const sock = dgram.createSocket("udp4");
    let timer;

    const uuid = `urn:uuid:${cryptoRandom()}`;

    const msg = `<?xml version="1.0" encoding="UTF-8"?>
<e:Envelope
  xmlns:e="http://www.w3.org/2003/05/soap-envelope"
  xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing"
  xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery">
  <e:Header>
    <w:MessageID>${uuid}</w:MessageID>
    <w:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To>
    <w:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action>
  </e:Header>
  <e:Body>
    <d:Probe>
      <d:Types xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
        dn:NetworkVideoTransmitter
      </d:Types>
    </d:Probe>
  </e:Body>
</e:Envelope>`;

    // IMPORTANT:
    // Key ONVIF devices by the actual IP address from which
    // the UDP response was received.
    const responses = new Map();

    const finish = () => {
      clearTimeout(timer);
      try {
        sock.close();
      } catch {}

      resolve([...responses.values()]);
    };

    sock.on("message", (msgBuf, rinfo) => {
      try {
        const xml = msgBuf.toString();

        // The actual ONVIF device IP is the sender IP.
        const deviceIp = rinfo && rinfo.address;

        // Only accept IPv4 addresses.
        if (
          !deviceIp ||
          !/^\d{1,3}(\.\d{1,3}){3}$/.test(deviceIp)
        ) {
          return;
        }

        const xaddrs = allTags(xml, "XAddrs")
          .flatMap(x => x.split(/\s+/))
          .filter(Boolean);

        const scopes = allTags(xml, "Scopes")
          .flatMap(x => x.split(/\s+/))
          .filter(Boolean);

        const endpoint = xmlTag(xml, "Address");

        // If the same device responds more than once,
        // merge its information instead of creating duplicates.
        const existing = responses.get(deviceIp) || {
          ip: deviceIp,
          xaddrs: [],
          scopes: [],
          source: "onvif"
        };

        existing.ip = deviceIp;

        existing.xaddrs = [
          ...new Set([
            ...existing.xaddrs,
            ...xaddrs
          ])
        ];

        existing.scopes = [
          ...new Set([
            ...existing.scopes,
            ...scopes
          ])
        ];

        // Keep endpoint information if available.
        if (endpoint) {
          existing.endpoint = endpoint;
        }

        responses.set(deviceIp, existing);

        console.log(
          `[ONVIF] Discovered device ${deviceIp}` +
          (xaddrs.length
            ? ` -> ${xaddrs.join(", ")}`
            : "")
        );

      } catch (err) {
        console.error("[ONVIF] Response parsing error:", err.message);
      }
    });

    sock.on("error", err => {
      console.error("[ONVIF] UDP socket error:", err.message);
      finish();
    });

    sock.bind(0, ifaceAddress, () => {
      try {
        sock.setMulticastInterface(ifaceAddress);
      } catch {}

      const buf = Buffer.from(msg);

      // Multicast ONVIF discovery.
      sock.send(
        buf,
        0,
        buf.length,
        ONVIF_UDP_PORT,
        "239.255.255.250"
      );

      // Also unicast when a target IP was supplied.
      if (ip) {
        sock.send(
          buf,
          0,
          buf.length,
          ONVIF_UDP_PORT,
          ip
        );
      }
    });

    timer = setTimeout(finish, DISCOVERY_TIMEOUT);
  });
}
function cryptoRandom() {
  return `${Date.now()}-${Math.random().toString(16).slice(2)}-${Math.random().toString(16).slice(2)}`;
}

async function pingSweep() {
  if (process.env.SMARTNODE_PING_SWEEP !== "1") return [];
  const ips = [];
  for (const i of interfaces()) ips.push(...cidrHosts(i.address, i.netmask));
  const unique = [...new Set(ips)];
  const found = [];
  const concurrency = 32;
  for (let i = 0; i < unique.length; i += concurrency) {
    const batch = unique.slice(i, i + concurrency);
    const r = await Promise.all(batch.map(async ip => {
      const out = await exec("ping", ["-c", "1", "-W", "1", ip]);
      return out ? ip : null;
    }));
    found.push(...r.filter(Boolean));
  }
  return found;
}

async function scan() {
  const ifaces = interfaces();
  const arp = await arpNeighbors();
  const ping = await pingSweep();

  const byIp = new Map();
  for (const d of arp) byIp.set(d.ip, { ...d });
  for (const ip of ping) if (!byIp.has(ip)) byIp.set(ip, { ip, source:"ping" });

  // ONVIF discovery on each physical interface.
  const onvifResults = (await Promise.all(
    ifaces.map(i => onvifProbe("", i.address))
  )).flat();

  for (const o of onvifResults) {
    const existing = byIp.get(o.ip) || { ip:o.ip, source:"onvif" };
    existing.onvif = true;
    existing.xaddrs = [...new Set([...(existing.xaddrs || []), ...o.xaddrs])];
    existing.scopes = [...new Set([...(existing.scopes || []), ...o.scopes])];
    byIp.set(o.ip, existing);
  }

  // Probe ports only for candidates. Full ping sweep can be enabled above.
  const candidates = [...byIp.values()];
  await Promise.all(candidates.map(async d => {
    d.ports = await probePorts(d.ip);
    d.rtsp = d.ports.includes(554);
  }));

  // ONVIF details. Use discovered XAddr; if none, try standard device service.
  await Promise.all(candidates.filter(d => d.onvif).map(async d => {
    const xaddr = d.xaddrs?.find(x => /^https?:\/\//i.test(x)) || `http://${d.ip}/onvif/device_service`;
    const details = await onvifDetails(xaddr, d.ip);
    Object.assign(d, details);
    const c = vendorClassification(d);
    d.type = c.type;
    d.typeConfidence = c.confidence;
    d.xaddrs = [...new Set(d.xaddrs || [])];
  }));

  for (const d of candidates) {
    if (!d.type) {
      if (d.rtsp) d.type = "rtsp-device";
      else if (d.ports.some(p => [80,443,8080].includes(p))) d.type = "http-device";
      else d.type = "unknown";
      d.typeConfidence = "low";
    }
    d.onvif = !!d.onvif;
    d.rtsp = !!d.rtsp;
    d.recording = !!d.recording;
    d.replay = !!d.replay;
    d.manufacturer = d.manufacturer || "";
    d.model = d.model || "";
    d.firmware = d.firmware || "";
    d.scopes = d.scopes || [];
    d.capabilities = d.capabilities || [];
    d.xaddrs = d.xaddrs || [];
  }

  return candidates.sort((a,b) => a.ip.localeCompare(b.ip, undefined, {numeric:true}));
}

module.exports = { scan, interfaces };
