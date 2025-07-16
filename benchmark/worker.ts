import * as olmdb from 'olmdb';

export async function run({ db_dir, value_size, gets_per_transaction, puts_per_transaction, tasks_per_thread, key_count, duration, seed }) {

    // It appears that Bun does not automatically reseed the random number generator
    // when forking, so we need to set it manually to prevent each worker from
    // generating the same sequence of gets/puts.
    try {
        let jsc = await import("bun:jsc");
        jsc.setRandomSeed(seed);
    } catch (err) {
        console.log("Could not import bun:jsc:", err.message);
    }


    const putValues: Uint8Array[] = [];
    for(let i=0; i<10; i++) {
        putValues.push(new Uint8Array(value_size).fill((''+i).charCodeAt(0)));
    }
    try {
        olmdb.init(db_dir);
    } catch(e: any) {
        if (e.code !== "DUP_INIT") throw e;
    }

    let transactions = 0;
    let tries = 0;

    const tasks: Promise<void>[] = [];
    const endTime = Date.now() + duration * 1000;
    for (let i = 0; i < tasks_per_thread; i++) {
        tasks.push((async () => {
            while (Date.now() < endTime) {
                transactions++;
                await olmdb.transact(() => {
                    tries++;
                    for(let j = 0; j < gets_per_transaction; j++) {
                        const key = Math.floor(Math.random() * key_count).toString();
                        olmdb.get(key);
                    }
                    for(let j = 0; j < puts_per_transaction; j++) {
                        const key = Math.floor(Math.random() * key_count).toString();
                        olmdb.put(key, putValues[transactions % putValues.length]);
                    }
                });

            }
        })());
    }
    await Promise.all(tasks);
    return { transactions, retries: tries - transactions };

}

process.on('message', async (options: any) => {
    const result = await run(options);
    process.send!(result);
    process.exit(0);
});
