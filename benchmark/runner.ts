import { fork } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import * as olmdb from 'olmdb';

function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        gets_per_transaction: 2,
        puts_per_transaction: 1,
        threads: 8,
        tasks_per_thread: 16,
        value_size: 1000,
        key_count: 100000,
        db_dir: "./.olmdb_benchmark",
        duration: 15, // seconds
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        if (arg.startsWith('--')) {
            let [key, value]: (string|number)[] = arg.substring(2).split('=');
            key = key.replace(/-/g, '_'); // Convert dashes to underscores
            if (key in options) {
                value ||= args[++i];
                if (typeof options[key] === 'number') value = parseInt(value, 10);
                options[key] = value;
            } else {
                console.error(`Unknown option: ${key}`);
                process.exit(1);
            }
        }
    }
    return options;
}

async function main() {
    const options = parseArgs();

    // Delete benchmark database directory
    if (fs.existsSync(options.db_dir)) {
        fs.rmSync(options.db_dir, { recursive: true, force: true });
    }

    // Open database and create initial data
    olmdb.open(options.db_dir);
    console.warn('Populating database...');
    const value = new Uint8Array(options.value_size).fill('x'.charCodeAt(0));
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
    console.warn('Database populated with initial data.');

    let totalTransactions = 0;
    let totalRetries = 0;

    const resultPromises: Promise<any>[] = [];
    console.warn(`Starting ${options.threads} threads with ${options.tasks_per_thread} tasks each for ${options.duration} seconds...`);
    for (let i = 0; i < options.threads; i++) {
        const worker = fork(path.resolve(__dirname, 'worker.ts'));
        resultPromises.push(new Promise((resolve, reject) => {
            worker.on('message', (result: any) => {
                totalTransactions += result.transactions;
                totalRetries += result.retries;
                resolve(result);
            });
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0) {
                    reject(new Error(`Worker stopped with exit code ${code}`));
                }
            });
            worker.send(options);
        }));
    }
    await Promise.all(resultPromises);

    console.log(JSON.stringify({
        transactions: totalTransactions,
        retries: totalRetries,
    }));
}

main().catch(console.error);
