import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import fs from "node:fs/promises";

const execFileAsync = promisify(execFile);

const COMMANDS = ["7zz", "7z", "unrar"];

async function findExtractor() {
  for (const command of COMMANDS) {
    try {
      await execFileAsync("sh", ["-c", `command -v ${command}`]);
      return command;
    } catch {}
  }
  throw new Error("No archive extractor found. Install 7zz/7z or unrar.");
}

function parseSevenZipList(stdout) {
  const entries = [];
  let current = {};

  for (const line of stdout.split(/\\r?\\n/)) {
    if (line === "") {
      if (current.Path && current.Folder !== "+") {
        entries.push({
          path: current.Path,
          size: Number(current.Size || 0),
          directory: current.Folder === "+"
        });
      }
      current = {};
      continue;
    }

    const match = line.match(/^([^=]+) = (.*)$/);
    if (match) current[match[1]] = match[2];
  }

  if (current.Path && current.Folder !== "+") {
    entries.push({
      path: current.Path,
      size: Number(current.Size || 0),
      directory: false
    });
  }

  return entries;
}

export async function listArchive(archivePath) {
  const extractor = await findExtractor();

  if (extractor === "unrar") {
    const { stdout } = await execFileAsync(extractor, ["lb", archivePath], {
      maxBuffer: 16 * 1024 * 1024
    });
    return stdout
      .split(/\\r?\\n/)
      .filter(Boolean)
      .map((entry) => ({ path: entry, size: null, directory: entry.endsWith("/") }));
  }

  const { stdout } = await execFileAsync(extractor, ["l", "-slt", archivePath], {
    maxBuffer: 32 * 1024 * 1024
  });

  return parseSevenZipList(stdout);
}

export async function streamArchiveEntry(archivePath, entryPath) {
  const extractor = await findExtractor();

  if (extractor === "unrar") {
    return spawn(extractor, ["p", "-inul", archivePath, entryPath], {
      stdio: ["ignore", "pipe", "pipe"]
    });
  }

  return spawn(extractor, ["x", "-so", "-y", archivePath, entryPath], {
    stdio: ["ignore", "pipe", "pipe"]
  });
}

export async function fileExists(filePath) {
  try {
    const stat = await fs.stat(filePath);
    return stat.isFile();
  } catch {
    return false;
  }
}

export function safeResolve(root, requested) {
  const rootResolved = path.resolve(root);
  const result = path.resolve(rootResolved, requested);
  if (result !== rootResolved && !result.startsWith(rootResolved + path.sep)) {
    throw new Error("Path escapes configured root");
  }
  return result;
}
