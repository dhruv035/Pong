import { Pool, PoolClient, Notification } from "pg";
import { PongTransaction, QueuedPingEvent } from "./queue";

export class Database {
  private pool: Pool;

  constructor() {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
    });
  }

  private async withClient<T>(
    operation: (client: PoolClient) => Promise<T>
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      return await operation(client);
    } finally {
      client.release();
    }
  }

  async confirmTransaction(txHash: string, nonce: number): Promise<void> {
    return this.withTransaction(async (client) => {
      console.log("confirmTransaction", txHash, nonce);
      await client.query(
        "UPDATE pong_transactions SET status = $1 WHERE nonce = $2",
        ["confirmed", nonce]
      );
      await client.query(
        "UPDATE ping_events SET processed = true WHERE pong_tx_nonce = $1",
        [nonce]
      );
    });
  }

  private async withTransaction<T>(
    operation: (client: PoolClient) => Promise<T>
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await operation(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getPongTransaction(nonce: number): Promise<PongTransaction | null> {
    return this.withClient(async (client) => {
      const result = await client.query<PongTransaction>(
        "SELECT * FROM pong_transactions WHERE nonce = $1",
        [nonce]
      );
      if (result.rows.length == 0) return null;
      return result.rows[0];
    });
  }

  async getPingEvent(nonce: number): Promise<QueuedPingEvent | null> {
    return this.withClient(async (client) => {
      const result = await client.query<QueuedPingEvent>(
        "SELECT * FROM ping_events WHERE pong_tx_nonce = $1",
        [nonce]
      );
      if (result.rows.length == 0) return null;
      return result.rows[0];
    });
  }

  async getUnprocessedEvents(): Promise<QueuedPingEvent[]> {
    return this.withClient(async (client) => {
      const result = await client.query<QueuedPingEvent>(
        "SELECT * FROM ping_events WHERE processed = false ORDER BY block_number ASC"
      );
      return result.rows;
    });
  }

  async updatePongTransaction(
    nonce: number,
    updates: Record<string, any>
  ): Promise<void> {
    return this.withClient(async (client) => {
      // Get the field names and values from the updates object
      const fields = Object.keys(updates);
      if (fields.length === 0) return; // Nothing to update

      // Create the SET part of the query: "field1 = $1, field2 = $2, ..."
      const setClause = fields
        .map((field, index) => `${field} = $${index + 1}`)
        .join(", ");

      // Extract the values in the same order as the fields
      const values = fields.map((field) => updates[field]);

      await client.query(
        `UPDATE pong_transactions SET ${setClause} WHERE nonce = $${
          values.length + 1
        }`,
        [...values, nonce]
      );
    });
  }

  async preparePongTransaction(
    pingTxHash: string,
    pongTxHash: string,
    nonce: number,
    blockNumber: number,
    isReplacement: boolean,
    isPrepared: boolean
  ): Promise<void> {
    return this.withTransaction(async (client) => {
      if (isReplacement || isPrepared) {
        await client.query(
          "UPDATE pong_transactions SET status = $1, replacement_hash = $2, block_number = $3 WHERE nonce = $4",
          ["replacing", pongTxHash, blockNumber, nonce]
        );
      } else {
        await client.query(
          "INSERT INTO pong_transactions (tx_hash, nonce, block_number, ping_hash, status) VALUES ($1, $2, $3, $4, $5)",
          [pongTxHash, nonce, blockNumber, pingTxHash, "preparing"]
        );
      }
      await client.query(
        "UPDATE ping_events SET pong_tx_nonce = $1 WHERE tx_hash = $2",
        [nonce, pingTxHash]
      );
    });
  }

  async updatePingEvent(
    pingTxHash: string,
    updates: Record<string, any>
  ): Promise<void> {
    return this.withClient(async (client) => {
      // Get the field names and values from the updates object
      const fields = Object.keys(updates);
      if (fields.length === 0) return; // Nothing to update

      // Create the SET part of the query: "field1 = $1, field2 = $2, ..."
      const setClause = fields
        .map((field, index) => `${field} = $${index + 1}`)
        .join(", ");

      // Extract the values in the same order as the fields
      const values = fields.map((field) => updates[field]);

      // Add the pingTxHash as the last parameter
      values.push(pingTxHash);

      // Construct the final query
      const query = `UPDATE ping_events SET ${setClause} WHERE tx_hash = $${values.length}`;

      // Execute the query
      await client.query(query, values);
    });
  }

  async setupNotificationListener(
    onNotification: (payload: QueuedPingEvent) => void
  ): Promise<{ client: PoolClient; cleanup: () => Promise<void> }> {
    const client = await this.pool.connect();
    try {
      await client.query("LISTEN ping_events");

      // Using any here because pg types don't properly expose the notification event
      (client as any).on("notification", (msg: Notification) => {
        try {
          if (!msg.payload) {
            console.error("Received notification without payload");
            return;
          }
          const payload = JSON.parse(msg.payload);
          onNotification(payload);
        } catch (error) {
          console.error("Error processing notification:", error);
        }
      });

      const cleanup = async () => {
        try {
          await client.query("UNLISTEN ping_events");
          client.release();
        } catch (error) {
          console.error("Error cleaning up listener:", error);
        }
      };

      return { client, cleanup };
    } catch (error) {
      client.release();
      throw error;
    }
  }
}
