import http from "node:http";
import { URL } from "node:url";
import path from "node:path";
import { listArchive, streamArchiveEntry, fileExists, safeResolve } from "./archive.js";
import { listRemoteRar, findRemoteRarEntry, closeRemoteRarIterator } from "./remote-rar.js";

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);
const ROOT = path.resolve(process.env.PKGSTREAM_ROOT || "./archives");
const RAR_PASSWORD = process.env.PKGSTREAM_RAR_PASSWORD || undefined;
const ALLOW_REMOTE = process.env.PKGSTREAM_ALLOW_REMOTE === "1";

function sendJson(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(data)
  });
  res.end(data);
}

async function getArchive(requested) {
  if (!requested) throw new Error("Missing archive parameter");
  const archivePath = safeResolve(ROOT, requested);
  if (!(await fileExists(archivePath))) {
    const error = new Error("Archive not found");
    error.statusCode = 404;
    throw error;
  }
  return archivePath;
}


function getRemoteSources(url) {
  const sources = url.searchParams.getAll("source");
  if (sources.length) return sources;
  const archive = url.searchParams.get("archive");
  if (archive && /^https?:\/\//i.test(archive)) return [archive];
  return [];
}

function parseRange(range, total) {
  if (!range) return { start: 0, end: total - 1, partial: false };
  const match = range.match(/^bytes=(\d*)-(\d*)$/);
  if (!match) return null;
  let start = 0, end = total - 1;
  if (match[1] === "") {
    const suffix = Number(match[2]);
    if (!Number.isSafeInteger(suffix) || suffix <= 0) return null;
    start = Math.max(0, total - suffix);
  } else {
    start = Number(match[1]);
    if (!Number.isSafeInteger(start) || start >= total) return null;
    if (match[2] !== "") {
      end = Number(match[2]);
      if (!Number.isSafeInteger(end) || end < start) return null;
      end = Math.min(end, total - 1);
    }
  }
  return { start, end, partial: true };
}

async function streamWebEntry(req, res, total, body, cleanup = async () => {}) {
  const range = parseRange(req.headers.range, total);
  if (!range) { await cleanup(); res.writeHead(416, { "content-range": "bytes */" + total }); return res.end(); }
  const { start, end, partial } = range;
  const length = end - start + 1;
  res.writeHead(partial ? 206 : 200, {
    "content-type": "application/octet-stream", "accept-ranges": "bytes", "content-length": length,
    ...(partial ? { "content-range": "bytes " + start + "-" + end + "/" + total } : {})
  });
  if (req.method === "HEAD") { await cleanup(); return res.end(); }
  const reader = body.getReader();
  let position = 0, sent = 0;
  try {
    while (sent < length) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunkStart = position, chunkEnd = position + value.byteLength - 1;
      position += value.byteLength;
      if (chunkEnd < start) continue;
      if (chunkStart > end) break;
      const from = Math.max(0, start - chunkStart);
      const to = Math.min(value.byteLength, end - chunkStart + 1);
      const slice = value.subarray(from, to);
      if (!res.write(slice)) await new Promise((resolve) => res.once("drain", resolve));
      sent += slice.byteLength;
    }
    await reader.cancel(); await cleanup(); if (!res.writableEnded) res.end();
  } catch (error) {
    await cleanup(); if (!res.headersSent) sendJson(res, 500, { error: error.message }); else res.destroy(error);
  }
}

async function handleRemoteList(url, res) {
  if (!ALLOW_REMOTE) return sendJson(res, 403, { error: "Remote sources are disabled. Set PKGSTREAM_ALLOW_REMOTE=1 to enable them." });
  const sources = getRemoteSources(url);
  if (!sources.length) return null;
  const entries = await listRemoteRar(sources, { password: RAR_PASSWORD });
  return sendJson(res, 200, { sources, entries });
}

async function handleRemoteStream(req, res, url) {
  if (!ALLOW_REMOTE) return sendJson(res, 403, { error: "Remote sources are disabled. Set PKGSTREAM_ALLOW_REMOTE=1 to enable them." });
  const sources = getRemoteSources(url);
  const entryPath = url.searchParams.get("entry");
  if (!sources.length) return sendJson(res, 400, { error: "Missing source parameter" });
  if (!entryPath) return sendJson(res, 400, { error: "Missing entry parameter" });
  const found = await findRemoteRarEntry(sources, entryPath, { password: RAR_PASSWORD });
  if (!found) return sendJson(res, 404, { error: "Archive entry not found" });
  const total = found.entry.size();
  if (!Number.isSafeInteger(total) || total < 0) { await closeRemoteRarIterator(found.iterator); return sendJson(res, 500, { error: "RAR backend did not provide a usable entry size" }); }
  return streamWebEntry(req, res, total, found.entry.body(), () => closeRemoteRarIterator(found.iterator));
}

async function handleList(url, res) {
  const remote = await handleRemoteList(url, res);
  if (remote) return remote;
  const archivePath = await getArchive(url.searchParams.get("archive"));
  const entries = await listArchive(archivePath);
  return sendJson(res, 200, { archive: path.relative(ROOT, archivePath), entries });
}

async function handleStream(req, res, url) {
  const archivePath = await getArchive(url.searchParams.get("archive"));
  const entryPath = url.searchParams.get("entry");
  if (!entryPath) return sendJson(res, 400, { error: "Missing entry parameter" });

  const entries = await listArchive(archivePath);
  const entry = entries.find((item) => item.path === entryPath && !item.directory);
  if (!entry) return sendJson(res, 404, { error: "Archive entry not found" });

  const total = entry.size;
  if (!Number.isSafeInteger(total) || total < 0) {
    return sendJson(res, 500, { error: "Archive backend did not provide a usable entry size" });
  }

  const range = req.headers.range;
  let start = 0;
  let end = total - 1;

  if (range) {
    const match = range.match(/^bytes=(\d*)-(\d*)$/);
    if (!match) {
      res.writeHead(416, { "content-range": `bytes */${total}` });
      return res.end();
    }

    if (match[1] === "") {
      const suffix = Number(match[2]);
      if (!Number.isSafeInteger(suffix) || suffix <= 0) {
        res.writeHead(416, { "content-range": `bytes */${total}` });
        return res.end();
      }
      start = Math.max(0, total - suffix);
    } else {
      start = Number(match[1]);
      if (!Number.isSafeInteger(start) || start >= total) {
        res.writeHead(416, { "content-range": `bytes */${total}` });
        return res.end();
      }
      if (match[2] !== "") {
        end = Math.min(total - 1, Number(match[2]));
      }
    }
  }

  const length = end - start + 1;
  const status = range ? 206 : 200;

  res.writeHead(status, {
    "content-type": "application/octet-stream",
    "accept-ranges": "bytes",
    "content-length": length,
    "content-range": range ? `bytes ${start}-${end}/${total}` : undefined
  });

  if (req.method === "HEAD") return res.end();

  const child = await streamArchiveEntry(archivePath, entryPath);
  let position = 0;
  let sent = 0;

  const cleanup = () => {
    if (!child.killed) child.kill("SIGTERM");
  };

  req.on("close", cleanup);
  res.on("close", cleanup);

  try {
    for await (const chunk of child.stdout) {
      const chunkStart = position;
      const chunkEnd = position + chunk.length - 1;
      position += chunk.length;

      if (chunkEnd < start) continue;
      if (chunkStart > end) break;

      const from = Math.max(0, start - chunkStart);
      const to = Math.min(chunk.length, end - chunkStart + 1);
      const slice = chunk.subarray(from, to);

      if (!res.write(slice)) {
        await new Promise((resolve) => res.once("drain", resolve));
      }

      sent += slice.length;
      if (sent >= length) break;
    }

    cleanup();
    if (!res.writableEnded) res.end();
  } catch (error) {
    cleanup();
    if (!res.headersSent) sendJson(res, 500, { error: error.message });
    else res.destroy(error);
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (url.pathname === "/health") {
      return sendJson(res, 200, {
        ok: true,
        service: "PKGStream",
        root: ROOT,
        remoteRar: true
      });
    }

    if (url.pathname === "/list" && (req.method === "GET" || req.method === "HEAD")) {
      return handleList(url, res);
    }

    if (url.pathname === "/stream" && (req.method === "GET" || req.method === "HEAD")) {
      if (getRemoteSources(url).length) return handleRemoteStream(req, res, url);
      return handleStream(req, res, url);
    }

    return sendJson(res, 404, { error: "Not found" });
  } catch (error) {
    const status = error.statusCode || 500;
    return sendJson(res, status, { error: error.message });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`PKGStream listening on http://${HOST}:${PORT}`);
  console.log(`Archive root: ${ROOT}`);
});
