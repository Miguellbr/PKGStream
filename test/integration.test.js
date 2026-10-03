import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";

const baseUrl = process.env.PKGSTREAM_BASE_URL || "http://127.0.0.1:8080";
const sourceUrl = process.env.PKGSTREAM_TEST_URL;

if (!sourceUrl) {
  test("remote integration suite", { skip: "set PKGSTREAM_TEST_URL to run remote integration tests" }, () => {});
} else {
  const source = new URL(sourceUrl);
  const expectedSize = Number(process.env.PKGSTREAM_EXPECTED_SIZE || 0);
  const cacheRoot = process.env.PKGSTREAM_CACHE_ROOT || path.resolve(".pkgstream-cache");

  function cacheKey() {
    return createHash("sha256")
      .update(JSON.stringify({ sources: [source.toString()], entryPath: source.searchParams.get("entry") }))
      .digest("hex");
  }

  async function removeCacheFiles() {
    const key = cacheKey();
    await Promise.all([
      rm(path.join(cacheRoot, key + ".bin"), { force: true }),
      rm(path.join(cacheRoot, key + ".json"), { force: true }),
      rm(path.join(cacheRoot, key + ".part"), { force: true })
    ]);
    return key;
  }

  async function request(url, options = {}) {
    const response = await fetch(url, options);
    const body = new Uint8Array(await response.arrayBuffer());
    return { response, body };
  }

  test("health endpoint", async () => {
    const { response, body } = await request(new URL("/health", baseUrl));
    assert.equal(response.status, 200);
    const parsed = JSON.parse(new TextDecoder().decode(body));
    assert.equal(parsed.ok, true);
    assert.equal(parsed.service, "PKGStream");
    assert.equal(parsed.remoteRar, true);
  });

  test("full remote stream", async () => {
    const { response, body } = await request(source);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("accept-ranges"), "bytes");
    assert.equal(Number(response.headers.get("content-length")), body.byteLength);
    if (expectedSize > 0) assert.equal(body.byteLength, expectedSize);
  });

  test("range from start", async () => {
    const url = new URL(source);
    const end = Math.min(1024 * 1024 - 1, expectedSize > 0 ? expectedSize - 1 : 1024 * 1024 - 1);
    const { response, body } = await request(url, { headers: { Range: `bytes=0-${end}` } });
    assert.equal(response.status, 206);
    assert.equal(body.byteLength, end + 1);
    assert.equal(response.headers.get("content-range"), `bytes 0-${end}/${expectedSize || response.headers.get("content-range").split("/")[1]}`);
  });

  test("middle range", async () => {
    const total = expectedSize || Number((await fetch(source, { method: "HEAD" })).headers.get("content-length"));
    assert.ok(total > 2_097_152);
    const start = Math.floor(total / 2);
    const end = Math.min(start + 1024 * 1024 - 1, total - 1);
    const { response, body } = await request(source, { headers: { Range: `bytes=${start}-${end}` } });
    assert.equal(response.status, 206);
    assert.equal(body.byteLength, end - start + 1);
    assert.equal(response.headers.get("content-range"), `bytes ${start}-${end}/${total}`);
  });

  test("concurrent full streams", async () => {
    const [a, b] = await Promise.all([request(source), request(source)]);
    assert.equal(a.response.status, 200);
    assert.equal(b.response.status, 200);
    assert.equal(a.body.byteLength, b.body.byteLength);
    if (expectedSize > 0) {
      assert.equal(a.body.byteLength, expectedSize);
      assert.equal(b.body.byteLength, expectedSize);
    }
  });

  test("invalid range returns 416", async () => {
    const { response } = await request(source, { headers: { Range: "bytes=999999999999-" } });
    assert.equal(response.status, 416);
  });

  test("missing entry returns 404", async () => {
    const bad = new URL(source);
    bad.searchParams.set("entry", "__pkgstream_integration_missing_entry__.pkg");
    const { response } = await request(bad);
    assert.equal(response.status, 404);
  });

  test("materialization error returns 502 and health recovers", async () => {
    const bad = new URL(source);
    bad.searchParams.set("archive", "https://example.com/pkgstream-integration-missing-archive.rar");
    bad.searchParams.set("entry", "pkg/foo.pkg");

    const { response } = await request(bad);
    assert.equal(response.status, 502);

    const health = await request(new URL("/health", baseUrl));
    assert.equal(health.response.status, 200);
    const parsed = JSON.parse(new TextDecoder().decode(health.body));
    assert.equal(parsed.ok, true);
  });

  test("stale partial cache is ignored and rebuilt", async () => {
    assert.ok(expectedSize > 0, "PKGSTREAM_EXPECTED_SIZE is required for cache recovery test");

    const key = await removeCacheFiles();
    await mkdir(cacheRoot, { recursive: true });
    const partPath = path.join(cacheRoot, key + ".part");
    await writeFile(partPath, Buffer.alloc(Math.min(1024 * 1024, expectedSize)));

    const { response, body } = await request(source);
    assert.equal(response.status, 200);
    assert.equal(body.byteLength, expectedSize);

    const dataInfo = await stat(path.join(cacheRoot, key + ".bin"));
    assert.equal(dataInfo.size, expectedSize);
  });
}
