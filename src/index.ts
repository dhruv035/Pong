import dotenv from "dotenv";
import EventQueue from "./queue";

// Load environment variables
dotenv.config();


async function main() {
  try {
    // Initialize database and event queue

    const eventQueue = new EventQueue();
    await eventQueue.initialize();
    eventQueue.processQueue();

    // Start listening for new events
    eventQueue.blockWatcher();
    eventQueue.startListening();

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
