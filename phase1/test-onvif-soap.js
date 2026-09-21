const http = require("http");
const crypto = require("crypto");

const IP = "10.10.10.5";
const USERNAME = "admin";
const PASSWORD = "SmartNode@2811";

if (!PASSWORD) {
  console.error("Set NVR_PASSWORD first");
  process.exit(1);
}

const created = new Date().toISOString();

const nonce = crypto.randomBytes(16);
const nonceBase64 = nonce.toString("base64");

const createdDate = created;

// ONVIF UsernameToken PasswordDigest
const digest = crypto
  .createHash("sha1")
  .update(Buffer.concat([
    nonce,
    Buffer.from(createdDate),
    Buffer.from(PASSWORD)
  ]))
  .digest("base64");

const soap = `<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope
  xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
  xmlns:tds="http://www.onvif.org/ver10/device/wsdl"
  xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
  xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">

  <soap:Header>
    <wsse:Security soap:mustUnderstand="true">
      <wsse:UsernameToken>
        <wsse:Username>${USERNAME}</wsse:Username>
        <wsse:Password
          Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.1#PasswordDigest">
          ${digest}
        </wsse:Password>
        <wsse:Nonce
          EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.1#Base64Binary">
          ${nonceBase64}
        </wsse:Nonce>
        <wsu:Created>${createdDate}</wsu:Created>
      </wsse:UsernameToken>
    </wsse:Security>
  </soap:Header>

  <soap:Body>
    <tds:GetDeviceInformation/>
  </soap:Body>

</soap:Envelope>`;

const options = {
  hostname: IP,
  port: 80,
  path: "/onvif/device_service",
  method: "POST",
  headers: {
    "Content-Type": "application/soap+xml; charset=utf-8",
    "Content-Length": Buffer.byteLength(soap)
  },
  timeout: 10000
};

const req = http.request(options, (res) => {
  console.log("HTTP status:", res.statusCode);
  console.log("Headers:", res.headers);

  let body = "";

  res.on("data", chunk => {
    body += chunk;
  });

  res.on("end", () => {
    console.log("\nResponse:\n");
    console.log(body);
  });
});

req.on("timeout", () => {
  console.error("Request timeout");
  req.destroy();
});

req.on("error", (err) => {
  console.error("Request error:", err.message);
});

req.write(soap);
req.end();
