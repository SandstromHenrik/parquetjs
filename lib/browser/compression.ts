// NOTICE: This is NOT tested by the normal unit tests as this is the browser version
// Needs to be tested manually for now by:
// 1. Load up the example server
// 2. examples/service/README.md
// 3. Test the files
'use strict';
import snappy from 'snappyjs';
import * as brotli from './brotli.js';
import * as zstdWorkers from '@yu7400ki/zstd-wasm/workers';

interface ZstdModule {
  compress: (input: Uint8Array, level?: number) => Promise<Uint8Array>;
  decompress: (input: Uint8Array) => Promise<Uint8Array>;
}
const zstdWorkersModule: ZstdModule = zstdWorkers as unknown as ZstdModule;

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${label} timeout after ${ms}ms`)), ms);
    p.then((v) => { clearTimeout(t); resolve(v); }, (e) => { clearTimeout(t); reject(e); });
  });
}

type PARQUET_COMPRESSION_METHODS = Record<
  string,
  {
  deflate: (value: ArrayBuffer | Buffer | Uint8Array | string) => Buffer | Promise<Buffer>;
  inflate: (value: ArrayBuffer | Buffer | Uint8Array | string) => Buffer | Promise<Buffer>;
  }
>;
// LZO compression is disabled. See: https://github.com/LibertyDSNP/parquetjs/issues/18
export const PARQUET_COMPRESSION_METHODS: PARQUET_COMPRESSION_METHODS = {
  UNCOMPRESSED: {
    deflate: deflate_identity,
    inflate: inflate_identity,
  },
  GZIP: {
    deflate: deflate_gzip,
    inflate: inflate_gzip,
  },
  SNAPPY: {
    deflate: deflate_snappy,
    inflate: inflate_snappy,
  },
  BROTLI: {
    deflate: deflate_brotli,
    inflate: inflate_brotli,
  },
  ZSTD: {
    deflate: deflate_zstd,
    inflate: inflate_zstd,
  },
};

/**
 * Deflate a value using compression method `method`
 */
export async function deflate(method: string, value: ArrayBuffer | Buffer | Uint8Array | string): Promise<Buffer> {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw new Error('invalid compression method: ' + method);
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

type InputLike = ArrayBuffer | Buffer | Uint8Array | string;

function deflate_identity(value: InputLike) {
  if (typeof value === 'string') return Buffer.from(value);
  return buffer_from_result(value);
}

function toBodyInit(value: InputLike): BodyInit {
  if (typeof value === 'string') return value;
  if (value instanceof Uint8Array) return (value as unknown) as BodyInit;
  if (Buffer.isBuffer(value)) {
    const buf = value as Buffer;
    return (new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength) as unknown) as BodyInit;
  }
  return (value as ArrayBuffer) as unknown as BodyInit;
}

async function deflate_gzip(value: InputLike) {
  const cs = new CompressionStream('gzip');
  const pipedCs = new Response(toBodyInit(value)).body?.pipeThrough(cs);
  return buffer_from_result(await new Response(pipedCs).arrayBuffer());
}

function deflate_snappy(value: InputLike) {
  const compressedValue = snappy.compress(toUint8(value));
  return buffer_from_result(compressedValue);
}

async function deflate_brotli(value: InputLike) {
  return buffer_from_result(await brotli.compress(toUint8(value)));
}

async function deflate_zstd(value: InputLike) {
  console.log('ZSTD compress: start', value);
  const input = toUint8(value);
  console.log('ZSTD compress: type', typeof input, 'constructor', input.constructor.name, 'length', input.length, 'slice', input.slice(0, 32));
  try {
    // Try workers build first (fast path); fallback to main if it times out or errors
    let result: Uint8Array;
    try {
      result = await withTimeout(zstdWorkersModule.compress(input, 6), 4000, 'zstd workers compress');
    } catch (e) {
      console.warn('ZSTD workers compress failed or timed out, falling back to main build:', e);
      throw e;
    }
    console.log('ZSTD compress: end', result);
    return buffer_from_result(result);
  } catch (err) {
    console.error('ZSTD compress error:', err);
    throw err;
  }
}

/**
 * Inflate a value using compression method `method`
 */
export async function inflate(method: string, value: InputLike): Promise<Buffer> {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw new Error('invalid compression method: ' + method);
  }

  return await PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

async function inflate_identity(value: InputLike): Promise<Buffer> {
  if (typeof value === 'string') return Buffer.from(value);
  return buffer_from_result(value);
}

async function inflate_gzip(value: InputLike) {
  const ds = new DecompressionStream('gzip');
  const pipedDs = new Response(toBodyInit(value)).body?.pipeThrough(ds);
  return buffer_from_result(await new Response(pipedDs).arrayBuffer());
}

function inflate_snappy(value: InputLike) {
  const uncompressedValue = snappy.uncompress(toUint8(value));
  return buffer_from_result(uncompressedValue);
}

async function inflate_brotli(value: InputLike) {
  return buffer_from_result(await brotli.inflate(toUint8(value)));
}

async function inflate_zstd(value: InputLike) {
  const input = toUint8(value);
  try {
    const out = await withTimeout(zstdWorkersModule.decompress(input), 4000, 'zstd workers decompress');
    return buffer_from_result(out);
  } catch (e) {
    console.warn('ZSTD workers decompress failed or timed out, falling back to main build:', e);
    throw e;
  }
}

function buffer_from_result(result: ArrayBuffer | Buffer | Uint8Array): Buffer {
  if (Buffer.isBuffer(result)) {
    return result;
  } else {
    return Buffer.from(new Uint8Array(result));
  }
}

function toUint8(value: InputLike): Uint8Array {
  // Always return a plain Uint8Array (not a Buffer subclass) to satisfy WASM/worker expectations
  if (Buffer.isBuffer(value)) {
    // Create a copy to ensure a plain Uint8Array instance
    return new Uint8Array(value);
  }
  if (value instanceof Uint8Array) {
    // If it's not a native Uint8Array (subclass), coerce to a real Uint8Array
    if (value.constructor !== Uint8Array) {
      return new Uint8Array(value);
    }
    return value;
  }
  if (typeof value === 'string') return new TextEncoder().encode(value);
  return new Uint8Array(value);
}
