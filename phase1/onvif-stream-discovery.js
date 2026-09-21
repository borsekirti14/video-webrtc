const { Cam } = require("onvif");

function discoverOnvifStreams({
  ip,
  username,
  password,
  port = 80
}) {
  return new Promise((resolve, reject) => {
    const camera = new Cam(
      {
        hostname: ip,
        port,
        username,
        password,
        timeout: 10000
      },
      function (err) {
        if (err) {
          return reject(err);
        }

        camera.getProfiles((err, profiles) => {
          if (err) {
            return reject(err);
          }

          const streams = [];
          let pending = profiles.length;

          if (!pending) {
            return resolve({
              profiles,
              streams
            });
          }

          profiles.forEach((profile, index) => {
            camera.getStreamUri(
              {
                protocol: "RTSP",
                profileToken: profile.$.token
              },
              (err, stream) => {
                streams.push({
                  profileIndex: index,
                  token: profile.$.token,
                  name: profile.name || "",
                  rtspUri: err ? null : stream.uri || null,
                  error: err ? err.message : null
                });

                pending--;

                if (pending === 0) {
                  resolve({
                    profiles,
                    streams
                  });
                }
              }
            );
          });
        });
      }
    );
  });
}

module.exports = {
  discoverOnvifStreams
};
