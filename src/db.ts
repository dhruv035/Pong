import { Pool, PoolClient, Notification } from "pg";
import { QueuedPingEvent } from "./queue";

export class Database {
    private pool: Pool;

    constructor(pool: Pool) {
        this.pool = pool;
    }

    private async withClient<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
        const client = await this.pool.connect();
        try {
            return await operation(client);
        } finally {
            client.release();
        }
    }

    async getLastNonce(): Promise<number> {
        return this.withClient(async (client) => {
            const result = await client.query<QueuedPingEvent>(
                'SELECT pong_tx_nonce FROM ping_events WHERE pong_tx_nonce IS NOT NULL ORDER BY pong_tx_nonce DESC LIMIT 1',
            );  
            if(result.rows.length==0)
                return 0;
            return result.rows[0].pong_tx_nonce??0;
        });
    }
    
    async getUnprocessedEvents(): Promise<QueuedPingEvent[]> {
        return this.withClient(async (client) => {
            const result = await client.query<QueuedPingEvent>(
                'SELECT * FROM ping_events WHERE processed = false ORDER BY block_number ASC'
            );
            return result.rows;
        });
    }

    async markEventAsProcessed(tx_hash: string): Promise<void> {
        return this.withClient(async (client) => {
            await client.query(
                'UPDATE ping_events SET processed = true WHERE tx_hash = $1',
                [tx_hash]
            );
        });
    }

    async updatePongTransaction(
        ping_tx_hash: string,
        pong_tx_hash: string,
        nonce: number,
        block_number: number
    ): Promise<void> {
        return this.withClient(async (client) => {
            await client.query(
                'UPDATE ping_events SET pong_tx_hash = $1, pong_tx_nonce = $2, pong_tx_block = $3 WHERE tx_hash = $4',
                [pong_tx_hash, nonce, block_number, ping_tx_hash]
            );
        });
    }

    async updateReplacementTransaction(
        ping_tx_hash: string,
        pong_tx_hash: string,
        block_number: number
    ): Promise<void> {
        return this.withClient(async (client) => {
            await client.query(
                'UPDATE ping_events SET pong_tx_hash = $1, pong_tx_block = $2 WHERE tx_hash = $3',
                [pong_tx_hash, block_number, ping_tx_hash]
            );
        });
    }


    async setupNotificationListener(
        onNotification: (payload: QueuedPingEvent) => void
    ): Promise<{ client: PoolClient; cleanup: () => Promise<void> }> {
        const client = await this.pool.connect();
        try {
            await client.query('LISTEN ping_events');

            // Using any here because pg types don't properly expose the notification event
            (client as any).on('notification', (msg: Notification) => {
                try {
                    if (!msg.payload) {
                        console.error('Received notification without payload');
                        return;
                    }
                    const payload = JSON.parse(msg.payload);
                    onNotification(payload);
                } catch (error) {
                    console.error('Error processing notification:', error);
                }
            });

            const cleanup = async () => {
                try {
                    await client.query('UNLISTEN ping_events');
                    client.release();
                } catch (error) {
                    console.error('Error cleaning up listener:', error);
                }
            };

            return { client, cleanup };
        } catch (error) {
            client.release();
            throw error;
        }
    }
}
