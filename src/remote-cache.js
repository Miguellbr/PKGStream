import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { mkdir, rename, rm, stat, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const CACHE_ROOT = path.resolve(process.env.PKGSTREAM_CACHE_ROOT || "./.pkgstream-cache");
const CACHE_TTL_MS = Number(process.env.PKGSTREAM_CACHE_TTL_MS || 24 * 60 * 60 * 1000);
const inflight = new Map();

function cacheKey(sources, entryPath) {
  return createHash("sha256")
    .update(JSON.stringify({ sources, entryPath }))
    .digest("hex");
}

function pathsFor(sources, entryPath) {
  const key = cacheKey(sources, entryPath);
  return {
    key,
    dataPath: path.join(CACHE_ROOT, `${key}.bin`),
    metaPath: path.join(CACHE_ROOT, `${key}.json`),
    partPath: path.join(CACHE_ROOT, `${key}.part`)
  };
}

async function readValidCache(sources, entryPath) {
  const paths = pathsFor(sources, entryPath);
  try {
    const [metaText, info] = await Promise.all([
      readFile(paths.metaPath, "utf8"),
      stat(paths.dataPath)
    ]);
    const meta = JSON.parse(metaText);
    if (meta.version !== 1 || meta.entryPath !== entryPath) return null;
    if (!Number.isSafeInteger(meta.size) || meta.size < 0 || info.size !== meta.size) return null;
    if (CACHE_TTL_MS > 0 && Date.now() - meta.createdAt > CACHE_TTL_MS) return null;
    return { ...paths, size: meta.size, createdAt: meta.createdAt, cacheHit: true };
  } catch {
    return null;
  }
}

export async function getRemoteCache(sources, entryPath) {
  return readValidCache(sources, entryPath);
}

export async function materializeRemoteEntry({ sources, entryPath, entry, iterator, total, debugLog }) {
  const existing = await readValidCache(sources, entryPath);
  if (existing) {
    await closeIterator(iterator);
    return existing;
  }

  const { key, dataPath, metaPath, partPath } = pathsFor(sources, entryPath);
  const current = inflight.get(key);
  if (current) {
    await closeIterator(iterator);
    return current;
  }

  const promise = (async () => {
    await mkdir(CACHE_ROOT, { recursive: true });
    await rm(partPath, { force: true });

    const startedAt = performance.now();
    let written = 0;
    try {
      const body = entry.body();
      if (!body || typeof body.getReader !== "function") {
        throw new Error("RAR backend did not provide a readable entry body");
      }

      const output = createWriteStream(partPath, { flags: "wx" });
      await pipeline(Readable.fromWeb(body), output);
      written = (await stat(partPath)).size;

      if (written !== total) {
        throw new Error(`Cached entry size mismatch: expected ${total}, got ${written}`);
      }

      await rename(partPath, dataPath);
      const createdAt = Date.now();
      await writeFile(metaPath, JSON.stringify({
        version: 1,
        entryPath,
        size: total,
        createdAt
      }) + "\n");

      debugLog?.("remote entry cached", {
        entry: entryPath,
        size: total,
        ms: Math.round(performance.now() - startedAt)
      });

      return { key, dataPath, metaPath, partPath, size: total, createdAt, cacheHit: false };
    } catch (error) {
      await rm(partPath, { force: true }).catch(() => {});
      await rm(metaPath, { force: true }).catch(() => {});
      await rm(dataPath, { force: true }).catch(() => {});
      throw error;
    } finally {
      await closeIterator(iterator);
    }
  })();

  inflight.set(key, promise);
  try {
    return await promise;
  } finally {
    inflight.delete(key);
  }
}

async function closeIterator(iterator) {
  if (iterator && typeof iterator.return === "function") {
    try { await iterator.return(); } catch {}
  }
}

export function createCachedRangeStream(cache, start, end) {
  return createReadStream(cache.dataPath, { start, end });
}

export function getCacheRoot() {
  return CACHE_ROOT;
}
