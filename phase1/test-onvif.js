const {
  discoverOnvifStreams
} = require("./onvif-stream-discovery");

async function main() {
  try {
    const result = await discoverOnvifStreams({
      ip: "10.10.10.5",
      username: "admin",
      password: process.env.NVR_PASSWORD
    });

    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error("ONVIF discovery failed:");
    console.error(error.message);
  }
}

main();
