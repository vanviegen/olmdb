import { fork } from 'child_process';
import * as fs from 'fs';
import * as olmdb from 'olmdb';
import * as path from 'path';
import { fileURLToPath } from 'url';

// @ts-ignore
if (typeof __dirname === 'undefined') global.__dirname = (typeof import.meta === 'undefined') ? process.cwd() : path.dirname(fileURLToPath(import.meta.url));

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        gets_per_transaction: 2,
        puts_per_transaction: 1,
        threads: 8,
        tasks_per_thread: 128,
        value_size: 100,
        key_count: 100000,
        db_dir: "./.olmdb_benchmark",
        duration: 10, // seconds
        delay: 0,
        keep: false,
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        if (arg.startsWith('--')) {
            let [key, value]: (string|number|boolean)[] = arg.substring(2).split('=');
            key = key.replace(/-/g, '_'); // Convert dashes to underscores
            if (key in options) {
                if (typeof options[key] === 'boolean') {
                    value = (value !== 'false' && value !== '0' && value !== 'no');
                } else {
                    value ||= args[++i];
                    if (typeof options[key] === 'number') value = parseInt(value, 10);
                }
                options[key] = value;
                continue;
            }
        }
        console.error(`Unknown option: ${arg}`);
        process.exit(1);

    }
    return options;
}

async function main() {
    const options = parseArgs();

    // Delete benchmark database directory
    if (options.key_count >= 0) {
        if (fs.existsSync(options.db_dir)) {
            fs.rmSync(options.db_dir, { recursive: true, force: true });
        }
    }
    // Open database and create initial data
    olmdb.init(options.db_dir);

    if (options.key_count >= 0) {
        console.warn('Populating database...');
        const value = new Uint8Array(options.value_size).fill('i'.charCodeAt(0));
        let cnt = 0;
        while(cnt < options.key_count) {
            // Max 10000 keys per transaction
            await olmdb.transact(() => {
                console.warn(`Adding keys ${cnt} to ${cnt + 10000}...`);
                for (let i = 0; i < 10000 && cnt < options.key_count; i++, cnt++) {
                    olmdb.put(cnt.toString(), value);
                }
            });
        }
    }

    if (options.delay > 0) {
        console.warn(`Waiting ${options.delay} seconds before starting benchmark...`);
        await sleep(options.delay * 1000);
    }

    let totals = {transactions: 0, retries: 0};

    const resultPromises: Promise<any>[] = [];
    console.warn(`Starting ${options.threads} threads with ${options.tasks_per_thread} tasks each for ${options.duration} seconds...`);
    const workerPath = path.resolve(__dirname, 'worker.ts');
    if (options.threads > 1) {
        for (let i = 0; i < options.threads; i++) {
            const seed = (i * 0xdeadbeef) ^ i;
            const worker = fork(workerPath);
            resultPromises.push(new Promise((resolve, reject) => {
                worker.on('message', (result: any) => {
                    totals.transactions += result.transactions;
                    totals.retries += result.retries;
                    resolve(result);
                });
                worker.on('error', reject);
                worker.on('exit', (code) => {
                    if (code !== 0) {
                        reject(new Error(`Worker stopped with exit code ${code}`));
                    }
                });
                worker.send({...options, seed});
            }));
        }
        await Promise.all(resultPromises);
    } else {
        const {run} = await import(workerPath);
        totals = await run({...options, seed: 0});
    }

    console.log(JSON.stringify(totals));
}

main().catch(console.error);
