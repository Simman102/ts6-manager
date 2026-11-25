import { spawn } from "child_process";
import path from "path";
import fs from "fs";
import { validateOpusFile } from "./opus-validator.js";
import { unwrapBbCodeUrls } from "../../utils/bbcode.js";

export interface YouTubeInfo {
  id: string;
  title: string;
  artist: string;
  duration: number; // seconds
  thumbnail: string;
  url: string;
}

export interface YouTubeSearchResult {
  id: string;
  title: string;
  artist: string;
  duration: number;
  thumbnail: string;
}

export interface DownloadYouTubeOptions {
  maxAttempts?: number;
  spawnFn?: typeof spawn;
}

// Single-flight deduplication maps so concurrent requests for the same URL or
// video ID share one download instead of racing on the same cache files.
const inFlightByUrl = new Map<string, Promise<{ filePath: string; info: YouTubeInfo }>>();
const inFlightById = new Map<string, Promise<{ filePath: string; info: YouTubeInfo }>>();

// Shared cookie file path (set from settings)
let ytCookieFile: string | null = null;

export function setYtCookieFile(filePath: string | null): void {
  ytCookieFile = filePath;
}

export function getYtCookieFile(): string | null {
  return ytCookieFile;
}

export function getCookieArgs(): string[] {
  const args: string[] = ["--remote-components", "ejs:github"];
  if (ytCookieFile) {
    args.push("--cookies", ytCookieFile);
  }
  return args;
}

function fetchYouTubeInfo(url: string, spawnFn: typeof spawn): Promise<YouTubeInfo> {
  return new Promise((resolve, reject) => {
    const infoProc = spawnFn(
      "yt-dlp",
      [...getCookieArgs(), "--dump-json", "--no-playlist", url],
      { shell: false },
    );

    let infoJson = "";
    let infoErr = "";
    infoProc.stdout?.on("data", (chunk: Buffer) => {
      infoJson += chunk.toString();
    });
    infoProc.stderr?.on("data", (chunk: Buffer) => {
      infoErr += chunk.toString();
    });

    infoProc.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(`yt-dlp info failed (code ${code}): ${infoErr.slice(0, 200)}`));
      }

      const firstLine = infoJson
        .trim()
        .split(/\r?\n/)
        .find((line) => line.trim().length > 0);
      if (!firstLine) {
        return reject(new Error("Failed to parse yt-dlp output"));
      }

      let parsed: any;
      try {
        parsed = JSON.parse(firstLine);
      } catch {
        return reject(new Error("Failed to parse yt-dlp output"));
      }

      if (!parsed?.id) {
        return reject(new Error("yt-dlp output missing video id"));
      }

      resolve({
        id: parsed.id,
        title: parsed.title || "Unknown",
        artist: parsed.uploader || parsed.channel || "Unknown",
        duration: parsed.duration || 0,
        thumbnail: parsed.thumbnail || "",
        url,
      });
    });

    infoProc.on("error", (err) => {
      reject(new Error(`yt-dlp not found: ${err.message}`));
    });
  });
}

function runYtDlpDownload(url: string, tempDir: string, spawnFn: typeof spawn): Promise<void> {
  return new Promise((resolve, reject) => {
    const outputTemplate = path.join(tempDir, "%(id)s.%(ext)s");
    const dlProc = spawnFn(
      "yt-dlp",
      [
        ...getCookieArgs(),
        "-x", // extract audio
        "--audio-format",
        "opus", // opus format (native for TS3)
        "--audio-quality",
        "0", // best quality
        "--no-playlist",
        "-o",
        outputTemplate,
        url,
      ],
      { shell: false },
    );

    let dlErr = "";
    dlProc.stderr?.on("data", (chunk: Buffer) => {
      dlErr += chunk.toString();
    });

    dlProc.on("close", (dlCode) => {
      if (dlCode !== 0) {
        return reject(new Error(`yt-dlp download failed (code ${dlCode}): ${dlErr.slice(0, 200)}`));
      }
      resolve();
    });

    dlProc.on("error", (err) => {
      reject(new Error(`yt-dlp not found: ${err.message}`));
    });
  });
}

async function cleanupPartialDebris(outputDir: string, videoId: string): Promise<void> {
  const entries = await fs.promises.readdir(outputDir).catch(() => [] as string[]);
  for (const entry of entries) {
    if (
      entry.startsWith(`${videoId}.`) &&
      (entry.endsWith(".part") || entry.endsWith(".ytdl") || entry.endsWith(".tmp"))
    ) {
      await fs.promises.unlink(path.join(outputDir, entry)).catch(() => {});
    }
  }
}

async function publishAtomically(tempFilePath: string, finalPath: string): Promise<void> {
  try {
    await fs.promises.rename(tempFilePath, finalPath);
  } catch (err: any) {
    if (err?.code === "EPERM" || err?.code === "EEXIST") {
      await fs.promises.unlink(finalPath).catch(() => {});
      await fs.promises.rename(tempFilePath, finalPath);
    } else {
      throw err;
    }
  }
}

async function ensureDownloadedById(
  url: string,
  resolvedDir: string,
  info: YouTubeInfo,
  spawnFn: typeof spawn,
  maxAttempts: number,
): Promise<{ filePath: string; info: YouTubeInfo }> {
  await fs.promises.mkdir(resolvedDir, { recursive: true });

  const expectedPath = path.join(resolvedDir, `${info.id}.opus`);
  if (fs.existsSync(expectedPath)) {
    const check = await validateOpusFile(expectedPath, info.duration || undefined);
    if (check.valid) {
      return { filePath: expectedPath, info };
    }
    console.warn(
      `[YouTube] Cached file ${expectedPath} is invalid (${check.reason}); deleting and re-downloading`,
    );
    await fs.promises.unlink(expectedPath).catch(() => {});
    await cleanupPartialDebris(resolvedDir, info.id);
  }

  const safeId = info.id.replace(/[^A-Za-z0-9_-]/g, "_") || "track";
  let lastError: unknown;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    // Keep tempDir inside resolvedDir so rename is on the same filesystem/volume
    const tempDir = await fs.promises.mkdtemp(path.join(resolvedDir, `.dl-${safeId}-`));
    try {
      await runYtDlpDownload(url, tempDir, spawnFn);

      const entries = (await fs.promises.readdir(tempDir)).filter(
        (name) =>
          !name.startsWith(".") &&
          !name.endsWith(".part") &&
          !name.endsWith(".ytdl") &&
          !name.endsWith(".tmp"),
      );

      const picked =
        entries.find((f) => f === `${info.id}.opus`) ??
        entries.find((f) => f.toLowerCase().endsWith(".opus")) ??
        entries[0];

      if (!picked) {
        throw new Error("Downloaded file not found");
      }

      const tempFilePath = path.join(tempDir, picked);
      if (path.extname(picked).toLowerCase() === ".opus") {
        const check = await validateOpusFile(tempFilePath, info.duration || undefined);
        if (!check.valid) {
          throw new Error(`Downloaded Opus file failed integrity check: ${check.reason}`);
        }
      } else {
        const stats = await fs.promises.stat(tempFilePath);
        if (stats.size <= 0) {
          throw new Error("Downloaded audio file is empty");
        }
      }

      const finalPath = path.join(resolvedDir, path.basename(tempFilePath));
      await publishAtomically(tempFilePath, finalPath);
      return { filePath: finalPath, info };
    } catch (err: any) {
      lastError = err;
      if (attempt < maxAttempts) {
        console.warn(
          `[YouTube] Download attempt ${attempt}/${maxAttempts} failed for ${info.id} (${err?.message || err}); retrying`,
        );
      }
    } finally {
      await fs.promises.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

/**
 * Download audio from a YouTube URL using yt-dlp.
 * Deduplicates concurrent downloads, downloads atomically in a staging dir,
 * and validates cached/downloaded .opus files so corrupted tracks auto-redownload.
 */
export function downloadYouTube(
  url: string,
  outputDir: string,
  options: DownloadYouTubeOptions = {},
): Promise<{ filePath: string; info: YouTubeInfo }> {
  const resolvedDir = path.resolve(outputDir);
  const normalizedUrl = unwrapBbCodeUrls(url);
  const urlKey = `${resolvedDir}::${normalizedUrl}`;

  const existingByUrl = inFlightByUrl.get(urlKey);
  if (existingByUrl) {
    return existingByUrl;
  }

  const spawnFn = options.spawnFn ?? spawn;
  const maxAttempts = Math.max(1, options.maxAttempts ?? 2);

  const promise = (async () => {
    const info = await fetchYouTubeInfo(normalizedUrl, spawnFn);
    const idKey = `${resolvedDir}::${info.id}`;

    const existingById = inFlightById.get(idKey);
    if (existingById) {
      const res = await existingById;
      return { filePath: res.filePath, info: { ...res.info, url: normalizedUrl } };
    }

    const idPromise = ensureDownloadedById(
      normalizedUrl,
      resolvedDir,
      info,
      spawnFn,
      maxAttempts,
    ).finally(() => {
      if (inFlightById.get(idKey) === idPromise) {
        inFlightById.delete(idKey);
      }
    });

    inFlightById.set(idKey, idPromise);
    return await idPromise;
  })().finally(() => {
    if (inFlightByUrl.get(urlKey) === promise) {
      inFlightByUrl.delete(urlKey);
    }
  });

  inFlightByUrl.set(urlKey, promise);
  return promise;
}

/**
 * Get info about a YouTube URL (single video or playlist).
 * Returns type ('video' or 'playlist') and array of items.
 */
export function getYouTubeUrlInfo(url: string): Promise<{ type: 'video' | 'playlist'; items: YouTubeSearchResult[] }> {
  const normalizedUrl = unwrapBbCodeUrls(url);
  return new Promise((resolve, reject) => {
    const proc = spawn("yt-dlp", [
        ...getCookieArgs(),
        "--dump-json",
      "--flat-playlist",
      "--no-download",
      normalizedUrl,
    ], { shell: false });

    let output = "";
    let stderr = "";
    proc.stdout.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(`yt-dlp info failed (code ${code}): ${stderr.slice(0, 200)}`));
      }

      try {
        const lines = output.trim().split("\n").filter(Boolean);
        const items: YouTubeSearchResult[] = lines.map((line) => {
          const parsed = JSON.parse(line);
          return {
            id: parsed.id,
            title: parsed.title || "Unknown",
            artist: parsed.uploader || parsed.channel || "Unknown",
            duration: parsed.duration || 0,
            thumbnail: parsed.thumbnails?.[0]?.url || parsed.thumbnail || "",
          };
        });

        const type = items.length > 1 ? 'playlist' : 'video';
        resolve({ type, items });
      } catch {
        reject(new Error("Failed to parse yt-dlp output"));
      }
    });

    proc.on("error", (err) => {
      reject(new Error(`yt-dlp not found: ${err.message}`));
    });
  });
}

/**
 * Search YouTube using yt-dlp
 */
export function searchYouTube(query: string, maxResults: number = 10): Promise<YouTubeSearchResult[]> {
  return new Promise((resolve, reject) => {
    const proc = spawn("yt-dlp", [
        ...getCookieArgs(),
        `ytsearch${maxResults}:${query}`,
      "--dump-json",
      "--flat-playlist",
      "--no-download",
    ], { shell: false });

    let output = "";
    let stderr = "";
    proc.stdout.on("data", (chunk: Buffer) => {
      output += chunk.toString();
    });
    proc.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        return reject(new Error(`yt-dlp search failed (code ${code}): ${stderr.slice(0, 200)}`));
      }

      try {
        // yt-dlp outputs one JSON object per line
        const results = output
          .trim()
          .split("\n")
          .filter(Boolean)
          .map((line) => {
            const parsed = JSON.parse(line);
            return {
              id: parsed.id,
              title: parsed.title || "Unknown",
              artist: parsed.uploader || parsed.channel || "Unknown",
              duration: parsed.duration || 0,
              thumbnail: parsed.thumbnails?.[0]?.url || "",
            };
          });

        resolve(results);
      } catch {
        resolve([]);
      }
    });

    proc.on("error", (err) => {
      reject(new Error(`yt-dlp not found: ${err.message}`));
    });
  });
}
