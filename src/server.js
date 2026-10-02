import http from "node:http";
import { URL } from "node:url";

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

function sendJson(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(data)
  });
  res.end(data);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health") {
    return sendJson(res, 200, { ok: true, service: "PKGStream" });
  }

  if (url.pathname === "/") {
    return sendJson(res, 200, {
      service: "PKGStream",
      status: "prototype",
      endpoints: ["/health"]
    });
  }

  return sendJson(res, 404, { error: "Not found" });
});

server.listen(PORT, HOST, () => {
  console.log(`PKGStream listening on http://${HOST}:${PORT}`);
});
