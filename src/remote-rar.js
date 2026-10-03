import { fromFetch, unrar } from "@mary/rar";

function readEntryValue(entry, name) {
  const value = entry[name];
  return typeof value === "function" ? value.call(entry) : value;
}

function assertHttpUrl(value) {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("Remote archive source must use HTTP or HTTPS");
  return url;
}

export async function resolveRemoteSource(value) {
  const url = assertHttpUrl(value);
  if (!/^(www\.)?mediafire\.com$/i.test(url.hostname)) return url.toString();

  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) throw new Error(`Remote source page returned HTTP ${response.status}`);
  const html = await response.text();

    const matches = [...html.matchAll(/https?:\/\/download\d+\.mediafire\.com\/[^"\'<>\s]+/gi)];
  if (!matches.length) throw new Error("Could not resolve a direct MediaFire download URL");

  return matches[0][0].replace(/&amp;/g, "&");
}

async function createReaders(urls) {
  if (!urls.length) throw new Error("Missing remote archive source");
  const resolved = [];
  for (const value of urls) resolved.push(await resolveRemoteSource(value));
  return Promise.all(resolved.map((value) => fromFetch({ input: assertHttpUrl(value) })));
}

export async function listRemoteRar(urls, options = {}) {
  const readers = await createReaders(urls);
  const source = readers.length === 1 ? readers[0] : readers;
  const entries = [];
  for await (const entry of unrar(source, options)) {
    entries.push({
      path: readEntryValue(entry, "filename"),
      size: readEntryValue(entry, "size"),
      directory: readEntryValue(entry, "isDirectory"),
      compressedSize: readEntryValue(entry, "compressedSize"),
      split: readEntryValue(entry, "isSplit"),
      encrypted: readEntryValue(entry, "isEncrypted"),
      solid: readEntryValue(entry, "isSolid")
    });
  }
  return entries;
}

export async function findRemoteRarEntry(urls, entryPath, options = {}) {
  const readers = await createReaders(urls);
  const source = readers.length === 1 ? readers[0] : readers;
  const iterator = unrar(source, options);
  while (true) {
    const result = await iterator.next();
    if (result.done) return null;
    if (!readEntryValue(result.value, "isDirectory") && readEntryValue(result.value, "filename") === entryPath) return { entry: result.value, iterator };
  }
}

export async function closeRemoteRarIterator(iterator) {
  if (iterator && typeof iterator.return === "function") {
    try { await iterator.return(); } catch {}
  }
}
