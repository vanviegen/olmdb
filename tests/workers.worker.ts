// Worker script used by tests/workers.test.ts. Loaded via the
// `node:worker_threads` Worker constructor (file mode, not eval) so it works
// reliably under both Node.js (Jest) and Bun.
import { parentPort, workerData } from "node:worker_threads";

interface Msg { dbDir: string; prefix: string; otherPrefix: string; recordCount: number; lowlevelUrl: string; }

(async () => {
    const data = workerData as Msg;
    const port = parentPort!;
    try {
        const lowlevel: any = await import(data.lowlevelUrl);
        lowlevel.init(data.dbDir);
        const enc = new TextEncoder();
        const dec = new TextDecoder();

        for (let i = 0; i < data.recordCount; i++) {
            const txn = lowlevel.startTransaction();
            const key = enc.encode(`${data.prefix}:${i.toString().padStart(6, "0")}`).buffer;
            const val = enc.encode(`v-${data.prefix}-${i}`).buffer;
            lowlevel.put(txn, key, val);
            await lowlevel.commitTransaction(txn);
        }
        port.postMessage({ phase: "wrote" });

        await new Promise<void>((r) => port.once("message", () => r()));

        const txn = lowlevel.startTransaction();
        let foundOwn = 0, foundOther = 0;
        for (let i = 0; i < data.recordCount; i++) {
            const k1 = enc.encode(`${data.prefix}:${i.toString().padStart(6, "0")}`).buffer;
            const v1 = lowlevel.get(txn, k1);
            if (v1 !== undefined && dec.decode(v1) === `v-${data.prefix}-${i}`) foundOwn++;
            const k2 = enc.encode(`${data.otherPrefix}:${i.toString().padStart(6, "0")}`).buffer;
            const v2 = lowlevel.get(txn, k2);
            if (v2 !== undefined && dec.decode(v2) === `v-${data.otherPrefix}-${i}`) foundOther++;
        }
        lowlevel.commitTransaction(txn);
        port.postMessage({ phase: "verified", foundOwn, foundOther });

        // Wait for the parent to acknowledge it received "verified", then exit
        // ourselves. Two failure modes this avoids:
        //  - The parent must NOT call worker.terminate(): under Bun, terminate()
        //    on a worker that loaded the native addon intermittently hangs (the
        //    parent spins with no OLMDB code on any stack), timing the test out.
        //  - We must NOT process.exit() immediately after postMessage either:
        //    under Node the in-flight "verified" message can be dropped when the
        //    worker exits, so the parent never sees it. Exiting only after the
        //    parent's ack guarantees the message was delivered on both runtimes.
        await new Promise<void>((r) => port.once("message", () => r()));
        process.exit(0);
    } catch (e: any) {
        port.postMessage({ phase: "error", error: String(e?.stack || e) });
    }
})();
