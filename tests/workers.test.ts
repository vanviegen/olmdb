import { describe, test, expect } from "@jest/globals";
import { Worker } from "node:worker_threads";
import { rmSync } from "node:fs";

// Exercise the per-Worker `transaction_client_t` instance code path in the
// napi module. Two Worker threads each call `lowlevel.init()` independently,
// so each gets its own client struct stored in its napi_env instance data.
// They share the same on-disk database via the single commit-worker daemon,
// so writes performed by one Worker must become visible to the other once
// the commit has landed.
//
// The main thread does not call `lowlevel.init()` itself, because each LMDB
// environment reserves a large amount of virtual address space and Jest's
// `--runInBand` mode shares the main process across multiple test files.

const DB_DIR = "./.olmdb_workers_test";
const RECORDS_PER_WORKER = 250;

interface WorkerHandle { worker: Worker; nextMessage(): Promise<any>; }

function spawnWorker(prefix: string, otherPrefix: string): WorkerHandle {
    // Under Bun, tests run directly from the .ts source; under Node/Jest the
    // compiled .js in dist/ is used. Pick the correct sibling for both the
    // worker entry point and the lowlevel module URL the worker will import.
    const isBun = typeof (globalThis as any).Bun !== "undefined";
    const workerUrl = isBun
        ? new URL("./workers.worker.ts", import.meta.url)
        : new URL("./workers.worker.js", import.meta.url);
    const lowlevelUrl = (isBun
        ? new URL("../src/lowlevel.ts", import.meta.url)
        : new URL("../src/lowlevel.js", import.meta.url)
    ).href;

    const w = new Worker(workerUrl, {
        workerData: {
            dbDir: DB_DIR,
            prefix,
            otherPrefix,
            recordCount: RECORDS_PER_WORKER,
            lowlevelUrl,
        },
    });
    const queue: any[] = [];
    const waiters: Array<(m: any) => void> = [];
    w.on("message", (m) => {
        if (waiters.length) waiters.shift()!(m);
        else queue.push(m);
    });
    w.on("error", (e) => {
        if (waiters.length) waiters.shift()!({ phase: "error", error: String(e) });
    });
    return {
        worker: w,
        nextMessage: () =>
            new Promise((resolve) => {
                if (queue.length) resolve(queue.shift());
                else waiters.push(resolve);
            }),
    };
}

describe("Multi-Worker thread safety", () => {
    test("each Worker uses an independent transaction_client_t instance", async () => {
        rmSync(DB_DIR, { recursive: true, force: true });

        const a = spawnWorker("wA", "wB");
        const b = spawnWorker("wB", "wA");

        const writeA = await a.nextMessage();
        const writeB = await b.nextMessage();
        if (writeA.phase === "error") console.error("A:", writeA.error);
        if (writeB.phase === "error") console.error("B:", writeB.error);
        expect(writeA.phase).toBe("wrote");
        expect(writeB.phase).toBe("wrote");

        a.worker.postMessage("verify");
        b.worker.postMessage("verify");

        const verA = await a.nextMessage();
        const verB = await b.nextMessage();

        expect(verA.phase).toBe("verified");
        expect(verA.foundOwn).toBe(RECORDS_PER_WORKER);
        expect(verA.foundOther).toBe(RECORDS_PER_WORKER);

        expect(verB.phase).toBe("verified");
        expect(verB.foundOwn).toBe(RECORDS_PER_WORKER);
        expect(verB.foundOther).toBe(RECORDS_PER_WORKER);

        await a.worker.terminate();
        await b.worker.terminate();
    }, 60000);
});
