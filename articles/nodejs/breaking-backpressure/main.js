import { Readable, Transform, Writable } from "stream";
import { pipeline } from "stream/promises";
console.log(`Starting our stream pipeline journey!`);
/**
 * Generate a series of events with increasing id
 *
 * @param size The number of elements to generate
 * @returns A {@link Generator} with a bunch of simple objects
 */
async function* generate(size) {
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
function work(id) {
    if (id % 8 === 0) {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve(Date.now());
            }, 1);
        });
    }
    return Date.now();
}
/**
 * Something that is semaphore like
 */
class Semaphoreish {
    _running = 0;
    _size;
    _resolvers = [];
    constructor(size) {
        this._size = size;
    }
    // How many are running
    get running() {
        return this._running;
    }
    /**
     * Acquire a lock
     */
    async acquire() {
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
    release() {
        this._running--;
        const next = this._resolvers.shift();
        if (next) {
            next();
        }
    }
}
class ConcurrentTransform extends Transform {
    onFinal;
    _semaphore;
    constructor() {
        super({ objectMode: true });
        this._semaphore = new Semaphoreish(this.readableHighWaterMark);
        this.on("data", (_) => {
            this._semaphore.release();
            this._checkDone();
        });
    }
    _final(callback) {
        // Trap the callback
        this.onFinal = callback;
        this._checkDone();
    }
    _checkDone() {
        // Check if we are done with everything before calling
        if (this._semaphore.running === 0 && this.onFinal !== undefined) {
            this.onFinal();
        }
    }
    async _transform(chunk, _encoding, callback) {
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
// let semaphore = new Semaphoreish(8);
// const transform = new Transform({
//   objectMode: true,
//   async transform(chunk, _encoding, callback) {
//     // Wait for our turn
//     await semaphore.acquire();
//     callback(); // Fire away to get more data
//     // // Push the value after resolving the work
//     this.push({ ...chunk, changedAt: await work(chunk.id) });
//     // // Don't forget to let someone else in
//     semaphore.release();
//   },
// });
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
