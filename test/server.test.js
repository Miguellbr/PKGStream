import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { spawn } from "node:child_process";

const port = 18080;

test("health endpoint", async () => {
  const child = spawn(process.execPath, ["src/server.js"], {
    env: { ...process.env, HOST: "127.0.0.1", PORT: String(port) },
    stdio: ["ignore", "pipe", "pipe"]
  });

  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("server timeout")), 5000);
      child.stdout.on("data", (chunk) => {
        if (chunk.toString().includes("PKGStream listening")) {
          clearTimeout(timer);
          resolve();
        }
      });
      child.on("error", reject);
      child.on("exit", (code) => {
        if (code !== null && code !== 0) reject(new Error(`server exited: ${code}`));
      });
    });

    const body = await new Promise((resolve, reject) => {
      http.get(`http://127.0.0.1:${port}/health`, (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => { data += chunk; });
        res.on("end", () => resolve({ status: res.statusCode, data }));
      }).on("error", reject);
    });

    assert.equal(body.status, 200);
    const parsed = JSON.parse(body.data);
    assert.equal(parsed.ok, true);
    assert.equal(parsed.service, "PKGStream");
    assert.equal(parsed.remoteRar, true);
  } finally {
    child.kill();
  }
});
