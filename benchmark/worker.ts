import * as olmdb from '../src/olmdb';

process.on('message', async (msg: any) => {
    const { db_dir, value_size, gets_per_transaction, puts_per_transaction, tasks_per_thread, key_count, duration } = msg;
    const putValue = new Uint8Array(value_size).fill('x'.charCodeAt(0));
    olmdb.open(db_dir);

    let transactions = 0;
    let tries = 0;

    const tasks: Promise<void>[] = [];
    const endTime = Date.now() + duration;
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
                        olmdb.put(key, putValue);
                    }
                });

            }
        })());
    }
    await Promise.all(tasks);

    process.send!({ transactions, retries: tries - transactions });
    process.exit(0);
});
