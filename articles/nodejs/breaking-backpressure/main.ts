import { Readable, Transform, Writable, type TransformCallback } from "stream";
import { pipeline } from "stream/promises";

console.log(`Starting our stream pipeline journey!`);

/**
 * Generate a series of events with increasing id
 *
 * @param size The number of elements to generate
 * @returns A {@link Generator} with a bunch of simple objects
 */
async function* generate(
  size: number
): AsyncGenerator<{ id: number }, void, undefined> {
  for (let n = 0; n < size; ++n) {
    yield { id: n };
  }

  return;
}

/**
 * Simulate some work that may or may not be async
 *
 * @param id The task id
 * @returns A Promise or value, depending on the id
 */
function work(id: number): number | Promise<number> {
  if (id % 8 === 0) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(Date.now());
      }, 1);
    });
  }

  return Date.now();
}

// Promise resolver definition
type Resolver = (value: void | PromiseLike<void>) => void;

/**
 * Something that is semaphore like
 */
class Semaphoreish {
  private _running: number = 0;
  private _size: number;
  private _resolvers: Resolver[] = [];

  constructor(size: number) {
    this._size = size;
  }

  // How many are running
  get running(): number {
    return this._running;
  }

  /**
   * Acquire a lock
   */
  async acquire(): Promise<void> {
    // If there are more folks running than should be, wait my turn
    if (this._running >= this._size) {
      await new Promise((resolver) => {
        this._resolvers.push(resolver);
      });
    }

    this._running++;
  }

  /**
   * Release the lock that I got
   */
  release(): void {
    this._running--;
    const next = this._resolvers.shift();
    if (next) {
      next();
    }
  }
}

class ConcurrentTransform extends Transform {
  private onFinal?: (error?: Error | null | undefined) => void;
  private _semaphore: Semaphoreish;

  constructor() {
    super({ objectMode: true });

    this._semaphore = new Semaphoreish(this.readableHighWaterMark);

    this.on("data", (_) => {
      this._semaphore.release();
      this._checkDone();
    });
  }

  _final(callback: (error?: Error | null | undefined) => void): void {
    // Trap the callback
    this.onFinal = callback;
    this._checkDone();
  }

  private _checkDone(): void {
    // Check if we are done with everything before calling
    if (this._semaphore.running === 0 && this.onFinal !== undefined) {
      this.onFinal();
    }
  }

  async _transform(
    chunk: any,
    _encoding: BufferEncoding,
    callback: TransformCallback
  ): Promise<void> {
    await this._semaphore.acquire();
    callback();

    this.push({ ...chunk, changedAt: await work(chunk.id) });
    this._checkDone();
  }
}

// Create our readable of 10 objects
const readable = Readable.from(generate(150), {
  objectMode: true,
});

// Create a simple transform to add the changedAt timestamp
const transform = new ConcurrentTransform();

// Create our writable
const writable = new Writable({
  objectMode: true,
  write: (_chunk, _encoding, callback) => {
    // Print our object
    // console.log(`Received ${inspect(chunk, true, undefined, true)} (encoding:${encoding})`);

    // This MUST be called for every object
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(undefined);
        callback(); // Remember, we need to call this
      }, 50); // Wait 50 milliseconds
    });
  },
});

// Set some interval that gets you results
const interval = setInterval(() => {
  console.log(`
        readable:   ${readable.readableLength} / ${readable.readableHighWaterMark}
        transform:  ${transform.readableLength} / ${transform.readableHighWaterMark} || ${transform.writableLength} / ${transform.writableHighWaterMark}
        writable:   ${writable.writableLength} / ${writable.writableHighWaterMark}
    `);
}, 1_000);

// Pipe the readable to the writable via the transform and wait for it to complete
await pipeline(readable, transform, writable);

clearInterval(interval);
