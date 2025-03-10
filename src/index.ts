import dotenv from "dotenv";
import { Pool } from "pg";
import EventQueue from "./queue";
import { Database } from "./db";

// Load environment variables
dotenv.config();
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function main() {
  try {
    // Initialize database and event queue
    const db = new Database(pool);
    const eventQueue = new EventQueue(db);
    await eventQueue.initialize();
    eventQueue.processQueue();

    // Start listening for new events
    await eventQueue.startListening();

    // Handle graceful shutdown
    process.on("SIGINT", async () => {
      console.log("Shutting down...");
      await eventQueue.stopListening();
      process.exit(0);
    });

    process.on("SIGTERM", async () => {
      console.log("Shutting down...");
      await eventQueue.stopListening();
      process.exit(0);
    });
  } catch (error) {
    console.error("Error starting the application:", error);
    process.exit(1);
  }
}

main();
