import { Router, Request, Response } from 'express';
import { requireRole } from '../middleware/rbac.js';
import { AppError } from '../middleware/error-handler.js';
import { downloadYouTube, searchYouTube, getYouTubeUrlInfo, type YouTubeInfo } from '../voice/audio/youtube.js';
import { validateOpusFile } from '../voice/audio/opus-validator.js';
import { unwrapBbCodeUrls } from '../utils/bbcode.js';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';

const MUSIC_DIR = process.env.MUSIC_DIR || '/data/music';
const ALLOWED_EXTENSIONS = ['.mp3', '.wav', '.flac', '.ogg', '.opus', '.m4a', '.aac', '.wma', '.webm'];
const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB

// Ensure music directory exists
if (!fs.existsSync(MUSIC_DIR)) {
  fs.mkdirSync(MUSIC_DIR, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, MUSIC_DIR),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    cb(null, `${randomUUID()}${ext}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: MAX_FILE_SIZE },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (ALLOWED_EXTENSIONS.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${ext}`));
    }
  },
});

export const musicLibraryRoutes: Router = Router({ mergeParams: true });

musicLibraryRoutes.use(requireRole('admin'));

// GET /songs — List songs for this server
musicLibraryRoutes.get('/songs', async (req: Request, res: Response, next) => {
  try {
    const prisma = req.app.locals.prisma;
    const configId = parseInt(req.params.configId as string);
    const songs = await prisma.song.findMany({
      where: { serverConfigId: configId },
      orderBy: { createdAt: 'desc' },
    });
    res.json(songs);
  } catch (err) { next(err); }
});

// POST /upload — Upload audio file
musicLibraryRoutes.post('/upload', upload.single('file'), async (req: Request, res: Response, next) => {
  try {
    const prisma = req.app.locals.prisma;
    const configId = parseInt(req.params.configId as string);
    const file = req.file;
    if (!file) throw new AppError(400, 'No file uploaded');

    // Extract duration via ffprobe
    let duration: number | null = null;
    try {
      duration = await getAudioDuration(file.path);
    } catch { /* ignore duration extraction failure */ }

    // Parse title/artist from filename
    const baseName = path.basename(file.originalname, path.extname(file.originalname));
    let title = baseName;
    let artist: string | null = null;
    const dashIdx = baseName.indexOf(' - ');
    if (dashIdx > 0) {
      artist = baseName.substring(0, dashIdx).trim();
      title = baseName.substring(dashIdx + 3).trim();
    }

    const song = await prisma.song.create({
      data: {
        title,
        artist,
        duration,
        filePath: file.path,
        source: 'local',
        fileSize: file.size,
        serverConfigId: configId,
      },
    });

    res.status(201).json(song);
  } catch (err) { next(err); }
});

const songUpsertLocks = new Map<string, Promise<unknown>>();

async function withSongLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const prev = songUpsertLocks.get(key) ?? Promise.resolve();
  const next = prev.then(fn, fn);
  const tail = next.then(() => undefined, () => undefined);
  songUpsertLocks.set(key, tail);
  try {
    return await next;
  } finally {
    if (songUpsertLocks.get(key) === tail) {
      songUpsertLocks.delete(key);
    }
  }
}

async function isCachedAudioValid(filePath: string, duration?: number | null): Promise<boolean> {
  if (!fs.existsSync(filePath)) return false;
  if (path.extname(filePath).toLowerCase() === '.opus') {
    const check = await validateOpusFile(filePath, duration ?? undefined);
    return check.valid;
  }
  try {
    return fs.statSync(filePath).size > 0;
  } catch {
    return false;
  }
}

async function upsertYouTubeSong(
  prisma: any,
  configId: number,
  url: string,
  filePath: string,
  info: YouTubeInfo,
): Promise<{ song: any; created: boolean }> {
  const lockKey = `${configId}::${path.resolve(filePath)}`;
  return withSongLock(lockKey, async () => {
    const fileStats = fs.statSync(filePath);
    const existing = await prisma.song.findFirst({
      where: {
        serverConfigId: configId,
        OR: [{ sourceUrl: url }, { filePath }],
      },
    });

    if (existing) {
      const nextDuration = info.duration || existing.duration;
      const needsUpdate =
        existing.filePath !== filePath ||
        existing.fileSize !== fileStats.size ||
        existing.sourceUrl !== url ||
        (info.duration && existing.duration !== info.duration);

      if (needsUpdate) {
        const updated = await prisma.song.update({
          where: { id: existing.id },
          data: {
            filePath,
            fileSize: fileStats.size,
            sourceUrl: url,
            ...(nextDuration ? { duration: nextDuration } : {}),
          },
        });
        return { song: updated, created: false };
      }
      return { song: existing, created: false };
    }

    const song = await prisma.song.create({
      data: {
        title: info.title,
        artist: info.artist,
        duration: info.duration,
        filePath,
        source: 'youtube',
        sourceUrl: url,
        fileSize: fileStats.size,
        serverConfigId: configId,
      },
    });
    return { song, created: true };
  });
}

// DELETE /songs/:id — Delete song
musicLibraryRoutes.delete('/songs/:id', async (req: Request, res: Response, next) => {
  try {
    const prisma = req.app.locals.prisma;
    const id = parseInt(req.params.id as string);
    const song = await prisma.song.findUnique({ where: { id } });
    if (!song) throw new AppError(404, 'Song not found');

    // Remove file from disk only if no other DB song references it
    const otherRefs = await prisma.song.count({
      where: { filePath: song.filePath, id: { not: id } },
    });
    if (otherRefs === 0) {
      try {
        if (fs.existsSync(song.filePath)) {
          fs.unlinkSync(song.filePath);
        }
      } catch { /* ignore file deletion failure */ }
    }

    await prisma.song.delete({ where: { id } });
    res.json({ success: true });
  } catch (err) { next(err); }
});

// POST /youtube/search — Search YouTube
musicLibraryRoutes.post('/youtube/search', async (req: Request, res: Response, next) => {
  try {
    const { query } = req.body;
    if (!query) throw new AppError(400, 'query is required');
    const results = await searchYouTube(query, 10);
    res.json(results);
  } catch (err) { next(err); }
});

// POST /youtube/download — Download from YouTube
musicLibraryRoutes.post('/youtube/download', async (req: Request, res: Response, next) => {
  try {
    const prisma = req.app.locals.prisma;
    const configId = parseInt(req.params.configId as string);
    const { url } = req.body;
    const cleanUrl = url ? unwrapBbCodeUrls(String(url)) : '';
    if (!cleanUrl) throw new AppError(400, 'url is required');

    const { filePath, info } = await downloadYouTube(cleanUrl, MUSIC_DIR);
    const { song, created } = await upsertYouTubeSong(prisma, configId, cleanUrl, filePath, info);

    res.status(created ? 201 : 200).json(song);
  } catch (err) { next(err); }
});

// POST /youtube/info — Get info about a YouTube URL (video or playlist)
musicLibraryRoutes.post('/youtube/info', async (req: Request, res: Response, next) => {
  try {
    const { url } = req.body;
    const cleanUrl = url ? unwrapBbCodeUrls(String(url)) : '';
    if (!cleanUrl) throw new AppError(400, 'url is required');
    const info = await getYouTubeUrlInfo(cleanUrl);
    res.json(info);
  } catch (err) { next(err); }
});

// POST /youtube/download-batch — Download multiple YouTube videos
musicLibraryRoutes.post('/youtube/download-batch', async (req: Request, res: Response, next) => {
  try {
    const prisma = req.app.locals.prisma;
    const configId = parseInt(req.params.configId as string);
    const { urls } = req.body;
    if (!Array.isArray(urls) || urls.length === 0) throw new AppError(400, 'urls array is required');

    const results: any[] = [];
    const errors: string[] = [];

    for (const rawUrl of urls) {
      const url = unwrapBbCodeUrls(String(rawUrl));
      if (!url) continue;
      try {
        // Check if already downloaded and valid on disk
        const existing = await prisma.song.findFirst({
          where: { sourceUrl: url, serverConfigId: configId },
        });
        if (existing && (await isCachedAudioValid(existing.filePath, existing.duration))) {
          results.push(existing);
          continue;
        }

        const { filePath, info } = await downloadYouTube(url, MUSIC_DIR);
        const { song } = await upsertYouTubeSong(prisma, configId, url, filePath, info);
        results.push(song);
      } catch (err: any) {
        errors.push(`${url}: ${err.message}`);
      }
    }

    res.json({ results, errors, total: urls.length, downloaded: results.length });
  } catch (err) { next(err); }
});

// Helper: get audio duration via ffprobe
function getAudioDuration(filePath: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const proc = spawn('ffprobe', [
      '-v', 'quiet',
      '-print_format', 'json',
      '-show_format',
      filePath,
    ], { shell: false });

    let output = '';
    proc.stdout.on('data', (chunk: Buffer) => { output += chunk.toString(); });
    proc.on('close', (code) => {
      if (code !== 0) return reject(new Error('ffprobe failed'));
      try {
        const parsed = JSON.parse(output);
        resolve(parseFloat(parsed.format.duration) || 0);
      } catch {
        reject(new Error('Failed to parse ffprobe output'));
      }
    });
    proc.on('error', reject);
  });
}
