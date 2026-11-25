import fs from "fs";

export interface OpusValidationResult {
  valid: boolean;
  reason?: string;
  duration?: number;
  pages?: number;
}

const OGG_CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let r = i << 24;
    for (let j = 0; j < 8; j++) {
      r = (r & 0x80000000) !== 0 ? ((r << 1) ^ 0x04c11db7) : (r << 1);
    }
    table[i] = r >>> 0;
  }
  return table;
})();

function computeOggCrc(buf: Buffer, start: number, end: number): number {
  let crc = 0;
  for (let i = start; i < end; i++) {
    // Ogg CRC is computed with the 4-byte checksum field (bytes 22..25) zeroed
    const byte = i >= start + 22 && i < start + 26 ? 0 : buf[i];
    crc = ((crc << 8) ^ OGG_CRC_TABLE[((crc >>> 24) ^ byte) & 0xff]) >>> 0;
  }
  return crc >>> 0;
}

/**
 * Validate the structural and CRC integrity of an Ogg/Opus file buffer.
 * Detects truncation, corrupt pages, mid-stream serial switches, sequence gaps,
 * missing BOS/EOS markers, backwards granule positions, and truncated duration.
 */
export function validateOpusBuffer(
  buf: Buffer,
  expectedDurationSec?: number,
): OpusValidationResult {
  if (buf.length < 27) {
    return {
      valid: false,
      reason: "File is empty or too small to be a valid Ogg Opus stream",
    };
  }

  let offset = 0;
  let pages = 0;
  let streamSerial = 0;
  let expectedSeq = 0;
  let preSkip = 0;
  let lastGranule = 0n;
  let seenEos = false;

  while (offset < buf.length) {
    if (offset + 27 > buf.length) {
      return { valid: false, reason: `Truncated Ogg page header at page ${pages}` };
    }

    if (
      buf[offset] !== 0x4f ||
      buf[offset + 1] !== 0x67 ||
      buf[offset + 2] !== 0x67 ||
      buf[offset + 3] !== 0x53
    ) {
      return { valid: false, reason: `Invalid Ogg page magic at page ${pages}` };
    }

    const version = buf[offset + 4];
    if (version !== 0) {
      return {
        valid: false,
        reason: `Unsupported Ogg stream version ${version} at page ${pages}`,
      };
    }

    const headerType = buf[offset + 5];
    const granule = buf.readBigInt64LE(offset + 6);
    const serial = buf.readUInt32LE(offset + 14);
    const pageSeq = buf.readUInt32LE(offset + 18);
    const storedCrc = buf.readUInt32LE(offset + 22);
    const segCount = buf[offset + 26];

    if (offset + 27 + segCount > buf.length) {
      return { valid: false, reason: `Truncated Ogg segment table at page ${pages}` };
    }

    let bodyLen = 0;
    for (let i = 0; i < segCount; i++) {
      bodyLen += buf[offset + 27 + i];
    }

    const pageEnd = offset + 27 + segCount + bodyLen;
    if (pageEnd > buf.length) {
      return { valid: false, reason: `Truncated Ogg page body at page ${pages}` };
    }

    const calcCrc = computeOggCrc(buf, offset, pageEnd);
    if (calcCrc !== storedCrc) {
      return { valid: false, reason: `Ogg page CRC mismatch at page ${pages}` };
    }

    if (seenEos) {
      return {
        valid: false,
        reason: `Unexpected data after Ogg EOS page at page ${pages}`,
      };
    }

    const payloadOffset = offset + 27 + segCount;
    const isBos = (headerType & 0x02) !== 0;

    if (pages === 0) {
      if (!isBos) {
        return { valid: false, reason: "Missing Ogg BOS flag on page 0" };
      }
      if (pageSeq !== 0) {
        return { valid: false, reason: `Expected page sequence 0 on page 0, got ${pageSeq}` };
      }
      if (
        bodyLen < 19 ||
        buf.subarray(payloadOffset, payloadOffset + 8).toString("ascii") !== "OpusHead"
      ) {
        return { valid: false, reason: "Missing OpusHead header on page 0" };
      }
      preSkip = buf.readUInt16LE(payloadOffset + 10);
      streamSerial = serial;
    } else if (pages === 1) {
      if (serial !== streamSerial) {
        return {
          valid: false,
          reason: `Ogg bitstream serial mismatch at page 1 (expected ${streamSerial}, got ${serial})`,
        };
      }
      if (pageSeq !== 1) {
        return {
          valid: false,
          reason: `Ogg page sequence mismatch at page 1 (expected 1, got ${pageSeq})`,
        };
      }
      if (isBos) {
        return { valid: false, reason: "Unexpected BOS flag mid-stream at page 1" };
      }
      if (
        bodyLen < 8 ||
        buf.subarray(payloadOffset, payloadOffset + 8).toString("ascii") !== "OpusTags"
      ) {
        return { valid: false, reason: "Missing OpusTags header on page 1" };
      }
    } else {
      if (serial !== streamSerial) {
        return {
          valid: false,
          reason: `Ogg bitstream serial mismatch at page ${pages} (expected ${streamSerial}, got ${serial})`,
        };
      }
      if (pageSeq !== expectedSeq) {
        return {
          valid: false,
          reason: `Ogg page sequence mismatch at page ${pages} (expected ${expectedSeq}, got ${pageSeq})`,
        };
      }
      if (isBos) {
        return { valid: false, reason: `Unexpected BOS flag mid-stream at page ${pages}` };
      }
    }

    if (granule >= 0n) {
      if (granule < lastGranule) {
        return {
          valid: false,
          reason: `Ogg granule position moved backwards at page ${pages}`,
        };
      }
      lastGranule = granule;
    }

    if ((headerType & 0x04) !== 0) {
      seenEos = true;
    }

    expectedSeq = (expectedSeq + 1) >>> 0;
    pages++;
    offset = pageEnd;
  }

  if (pages < 3) {
    return { valid: false, reason: "Opus stream has too few Ogg pages" };
  }
  if (!seenEos) {
    return { valid: false, reason: "Missing Ogg EOS marker (file truncated)" };
  }
  if (lastGranule <= BigInt(preSkip)) {
    return { valid: false, reason: "Opus stream contains no audio samples" };
  }

  const duration = Number(lastGranule - BigInt(preSkip)) / 48000;

  if (expectedDurationSec !== undefined && expectedDurationSec > 0) {
    if (expectedDurationSec >= 10) {
      const tolerance = Math.min(30, Math.max(5, expectedDurationSec * 0.05));
      if (duration < expectedDurationSec - tolerance) {
        return {
          valid: false,
          reason: `Opus duration ${duration.toFixed(1)}s is shorter than expected ${expectedDurationSec.toFixed(1)}s`,
        };
      }
    } else if (expectedDurationSec > 2 && duration < expectedDurationSec * 0.5) {
      return {
        valid: false,
        reason: `Opus duration ${duration.toFixed(1)}s is shorter than expected ${expectedDurationSec.toFixed(1)}s`,
      };
    }
  }

  return { valid: true, duration, pages };
}

/**
 * Read an Ogg/Opus file from disk and validate its integrity.
 */
export async function validateOpusFile(
  filePath: string,
  expectedDurationSec?: number,
): Promise<OpusValidationResult> {
  try {
    const buf = await fs.promises.readFile(filePath);
    return validateOpusBuffer(buf, expectedDurationSec);
  } catch (err: any) {
    return {
      valid: false,
      reason: err?.message || "Failed to read Opus file",
    };
  }
}
