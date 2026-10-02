/*
 * Videodoorphone relay server (Render).
 *
 * Bridges the ESP32-S3 (on the home WiFi) and the companion app (on
 * cellular data), which cannot reach each other directly because both
 * are behind NAT. Also triggers the Pushy push notification when a
 * call starts.
 *
 * Protocol on the WebSocket connection (path: /ws):
 *   - First message from every client (text JSON) identifies its role:
 *       {"role":"esp32"}
 *       {"role":"app"}
 *   - Text frames after that are JSON control/event messages:
 *       ESP32 -> server : {"cmd":"call_request"}
 *       app   -> server : {"cmd":"answer"}   (app explicitly answering a
 *                                              call - the app may already
 *                                              have been connected before
 *                                              the call started, so this
 *                                              is not inferred just from
 *                                              the app's connection event)
 *       server -> ESP32 : {"event":"app_connected"}
 *       server -> ESP32 : {"event":"call_timeout"}
 *       app   -> server : {"cmd":"open_door"}
 *       server -> ESP32 : {"cmd":"open_door"}
 *   - Binary frames are 1 leading type byte + payload, relayed as-is:
 *       0x01 = video (ESP32 -> app only)
 *       0x02 = audio (both directions)
 *
 * Only one ESP32 and one app are expected at a time (single household
 * device). A new connection with the same role replaces the old one.
 *
 * GET /mjpeg: every video frame (0x01) received from the ESP32 is also
 * broadcast here as a standard MJPEG multipart stream, so the app can just
 * point a native WebViewer at this URL instead of needing a custom
 * component to draw the video - the official MIT App Inventor Designer
 * can't render a fully custom visible component (see the conversation this
 * was built from), but any browser/WebView already knows how to display an
 * MJPEG stream natively.
 */

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 10000;
const PUSHY_API_KEY = process.env.PUSHY_API_KEY;
const PUSHY_TOPIC = process.env.PUSHY_TOPIC || 'MyHome';
const CALL_TIMEOUT_MS = 60000;

const MJPEG_BOUNDARY = 'videocitofonoframe';
const mjpegClients = [];

const server = http.createServer((req, res) => {
  if (req.url === '/mjpeg') {
    res.writeHead(200, {
      'Content-Type': `multipart/x-mixed-replace; boundary=${MJPEG_BOUNDARY}`,
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Connection': 'close'
    });
    mjpegClients.push(res);
    log('MJPEG client connected, total:', mjpegClients.length);

    req.on('close', () => {
      const idx = mjpegClients.indexOf(res);
      if (idx !== -1) {
        mjpegClients.splice(idx, 1);
      }
      log('MJPEG client disconnected, total:', mjpegClients.length);
    });
    return;
  }

  // Plain HTTP endpoint, useful for uptime pings (Render free tier sleeps
  // after inactivity) and as a quick health check.
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('videodoorphone relay ok\n');
});

function broadcastMjpegFrame(jpegBuffer) {
  if (mjpegClients.length === 0) {
    return;
  }
  const header = Buffer.from(
    `--${MJPEG_BOUNDARY}\r\n` +
    'Content-Type: image/jpeg\r\n' +
    `Content-Length: ${jpegBuffer.length}\r\n\r\n`
  );
  const footer = Buffer.from('\r\n');

  // Iterate backwards so a client removed mid-loop (on write error) doesn't
  // shift the indices of the ones still to come.
  for (let i = mjpegClients.length - 1; i >= 0; i--) {
    const client = mjpegClients[i];
    try {
      client.write(header);
      client.write(jpegBuffer);
      client.write(footer);
    } catch (err) {
      mjpegClients.splice(i, 1);
    }
  }
}

const wss = new WebSocket.Server({ server, path: '/ws' });

let esp32Socket = null;
let appSocket = null;
let callState = 'idle'; // idle | calling | in_call
let callTimeoutHandle = null;

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function clearCallTimeout() {
  if (callTimeoutHandle) {
    clearTimeout(callTimeoutHandle);
    callTimeoutHandle = null;
  }
}

function sendJson(socket, obj) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(obj));
  }
}

async function notifyPushy() {
  if (!PUSHY_API_KEY) {
    log('PUSHY_API_KEY not set, skipping push notification');
    return;
  }

  try {
    const response = await fetch(`https://api.pushy.me/push?api_key=${PUSHY_API_KEY}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        to: `/topics/${PUSHY_TOPIC}`,
        data: {
          title: 'Videodoorphone',
          message: 'Incoming call',
          notificationId: Date.now().toString()
        }
      })
    });

    if (!response.ok) {
      const text = await response.text();
      log('Pushy notification failed:', response.status, text);
    } else {
      log('Pushy notification sent to topic', PUSHY_TOPIC);
    }
  } catch (err) {
    log('Pushy notification error:', err.message);
  }
}

function handleCallRequest() {
  if (callState !== 'idle') {
    // ESP32 already enforces "one call at a time" on its side, but the
    // server stays defensive in case of a stray/duplicate message.
    log('call_request ignored, call already in progress');
    return;
  }

  callState = 'calling';
  log('call_request received, notifying Pushy');
  notifyPushy();

  clearCallTimeout();
  callTimeoutHandle = setTimeout(() => {
    if (callState === 'calling') {
      log('call timed out, no app connected within', CALL_TIMEOUT_MS, 'ms');
      sendJson(esp32Socket, { event: 'call_timeout' });
      callState = 'idle';
    }
  }, CALL_TIMEOUT_MS);
}

function handleAppAnswered(reason) {
  if (callState === 'calling') {
    log('call answered (' + reason + ')');
    clearCallTimeout();
    callState = 'in_call';
    sendJson(esp32Socket, { event: 'app_connected' });
  }
}

function handleOpenDoor() {
  log('open_door forwarded to ESP32');
  sendJson(esp32Socket, { cmd: 'open_door' });
}

function resetCallState() {
  clearCallTimeout();
  callState = 'idle';
}

wss.on('connection', (socket) => {
  let role = null;

  socket.on('message', (data, isBinary) => {
    if (!isBinary) {
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch (err) {
        log('Invalid JSON received, ignoring');
        return;
      }

      // First text message identifies the role.
      if (role === null && msg.role) {
        role = msg.role;

        if (role === 'esp32') {
          if (esp32Socket && esp32Socket !== socket) {
            esp32Socket.close();
          }
          esp32Socket = socket;
          log('ESP32 connected');
        } else if (role === 'app') {
          if (appSocket && appSocket !== socket) {
            appSocket.close();
          }
          appSocket = socket;
          log('App connected');
          // Covers the common case: the app was closed, opened a fresh
          // connection specifically in response to the call notification.
          // If the app was already connected before the call started, it
          // must send an explicit {"cmd":"answer"} instead (below) -
          // connecting is not proof of answering in that case.
          handleAppAnswered('app connection');
        }
        return;
      }

      if (role === 'esp32' && msg.cmd === 'call_request') {
        handleCallRequest();
      } else if (role === 'app' && msg.cmd === 'open_door') {
        handleOpenDoor();
      } else if (role === 'app' && msg.cmd === 'answer') {
        handleAppAnswered('explicit answer command');
      }

      return;
    }

    // Binary media frame: relay as-is over the WebSocket, leading type byte
    // untouched (kept for the WebSocket extension path / audio, which still
    // uses it).
    if (role === 'esp32' && appSocket && appSocket.readyState === WebSocket.OPEN) {
      appSocket.send(data, { binary: true });
    } else if (role === 'app' && esp32Socket && esp32Socket.readyState === WebSocket.OPEN) {
      esp32Socket.send(data, { binary: true });
    }

    // Video frames (0x01) are also broadcast to any /mjpeg HTTP client,
    // independent of whether an app WebSocket is connected - the type byte
    // is stripped since MJPEG frames are just raw JPEG bytes.
    if (role === 'esp32' && data.length > 0 && data[0] === 0x01) {
      broadcastMjpegFrame(data.subarray(1));
    }
  });

  socket.on('close', () => {
    if (role === 'esp32' && esp32Socket === socket) {
      log('ESP32 disconnected');
      esp32Socket = null;
      resetCallState();
    } else if (role === 'app' && appSocket === socket) {
      log('App disconnected');
      appSocket = null;
      if (callState === 'in_call') {
        // The app hung up: bring the ESP32 back to idle too.
        sendJson(esp32Socket, { event: 'call_timeout' });
        resetCallState();
      }
    }
  });

  socket.on('error', (err) => {
    log('Socket error:', err.message);
  });
});

server.listen(PORT, () => {
  log('Relay server listening on port', PORT);
});
