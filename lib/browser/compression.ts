// NOTICE: This is NOT tested by the normal unit tests as this is the browser version
// Needs to be tested manually for now by:
// 1. Load up the example server
// 2. examples/service/README.md
// 3. Test the files
'use strict';
import snappy from 'snappyjs';
import * as brotli from './brotli.js';
// ZSTD: import both builds and choose at runtime to avoid dynamic import/code-splitting in Workers
import * as zstdWorkers from '@yu7400ki/zstd-wasm/workers';
import * as zstdMain from '@yu7400ki/zstd-wasm';

interface ZstdModule {
  compress: (input: Uint8Array, level?: number) => Promise<Uint8Array>;
  decompress: (input: Uint8Array) => Promise<Uint8Array>;
}
const isCloudflareWorker = typeof (globalThis as { WebSocketPair?: unknown }).WebSocketPair === 'function';
const hasDedicatedWorkers = typeof (globalThis as { Worker?: unknown }).Worker === 'function';
// Prefer non-worker build in Cloudflare Workers or when DedicatedWorker is unavailable
const zstdModule: ZstdModule = (isCloudflareWorker || !hasDedicatedWorkers)
  ? (zstdMain as unknown as ZstdModule)
  : (zstdWorkers as unknown as ZstdModule);

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
  const result = await zstdModule.compress(input, 3);
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
  return buffer_from_result(await zstdModule.decompress(input));
}

function buffer_from_result(result: ArrayBuffer | Buffer | Uint8Array): Buffer {
  if (Buffer.isBuffer(result)) {
    return result;
  } else {
    return Buffer.from(new Uint8Array(result));
  }
}

function toUint8(value: InputLike): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (Buffer.isBuffer(value)) {
    const buf = value as Buffer;
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  if (typeof value === 'string') return new TextEncoder().encode(value);
  return new Uint8Array(value);
}
