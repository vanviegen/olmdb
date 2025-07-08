import * as os from 'os';
import * as olmdb from 'olmdb';

// Just use numbers for keys, but stored as binary data
export function keyFromNumber(n: number): Uint8Array {
    const arr = new Uint8Array(4);
    const view = new DataView(arr.buffer);
    view.setUint32(0, n, false); // big-endian
    return arr;
}

export function numberFromKey(key: Uint8Array): number {
    const view = new DataView(key.buffer);
    return view.getUint32(0, false); // big-endian
}

export async function generateDataset(dbDir: string, numKeys: number, valueSize: number) {
    console.log(`Generating dataset with ${numKeys} keys of size ${valueSize}...`);
    olmdb.init(dbDir);
    const value = new Uint8Array(valueSize);
    await olmdb.transact(() => {
        for (let i = 0; i < numKeys; i++) {
            if (i > 0 && i % 10000 === 0) {
                console.log(`  ... generated ${i} keys`);
            }
            olmdb.put(keyFromNumber(i), value);
        }
    });
    console.log('Dataset generation complete.');
}

export function getSystemInfo() {
    const cpus = os.cpus();
    return {
        os: `${os.type()} ${os.release()}`,
        cpu: cpus[0].model,
        cpuCount: cpus.length,
        ram: `${(os.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB`,
        nodeVersion: process.versions.node,
        bunVersion: (process.versions as any).bun,
    };
}
