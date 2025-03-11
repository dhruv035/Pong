import { ethers } from "ethers";
import { abi } from "./abi";
import { Database } from "./db";

export type QueuedPingEvent = {
  timestamp: number;
  tx_hash: string;
  processed: boolean;
  pong_tx_hash: string | null;
  pong_tx_nonce: number | null;
  pong_tx_block: number | null;
  created_at: Date;
  block_number: number;
};

interface PongContract extends ethers.BaseContract {
  pong(
    txHash: string,
    overrides?: ethers.Overrides
  ): Promise<ethers.ContractTransactionResponse>;
}

class EventQueue {
  private queue: QueuedPingEvent[] = [];
  private isProcessing: boolean = false;
  private contract: PongContract;
  private db: Database;
  private cleanup: (() => Promise<void>) | null = null;
  private BLOCK_THRESHOLD = 5; // Number of blocks to wait before replacement
  private readonly MAX_GAS_PRICE = 250n * 1000000000n; // 250 gwei
  private readonly GAS_PRICE_CHECK_DELAY = 12000; // 12 seconds in milliseconds
  private readonly TX_CONFIRMATION_TIMEOUT = 60000; // 60 seconds timeout for transaction confirmation
  private wallet: ethers.Wallet;
  private provider: ethers.Provider;
  private ourAddress: string;

  constructor(db: Database) {
    this.db = db;
    const provider = new ethers.JsonRpcProvider(process.env.SEPOLIA_RPC_URL);
    this.provider = provider;

    const wallet = new ethers.Wallet(process.env.PRIVATE_KEY!, provider);
    this.wallet = wallet;
    this.ourAddress = wallet.address;

    // Initialize contract with signer
    this.contract = new ethers.Contract(
      process.env.CONTRACT_ADDRESS!,
      abi,
      wallet
    ) as unknown as PongContract;

    console.log(`Initialized with wallet address: ${this.ourAddress}`);
  }

  async initialize() {
    this.queue = await this.db.getUnprocessedEvents();
  }

  async startListening() {
    try {
      const { cleanup } = await this.db.setupNotificationListener(
        async (payload) => {
          console.log("New ping event received:", payload);
          await this.addEvent(payload);
          await this.processQueue();
        }
      );

      this.cleanup = cleanup;
      console.log("Event listener started successfully");
    } catch (error) {
      console.error("Error starting event listener:", error);
      throw error;
    }
  }

  async stopListening() {
    if (this.cleanup) {
      await this.cleanup();
      console.log("Event listener stopped successfully");
    }
  }

  async addEvent(event: QueuedPingEvent) {
    this.queue.push(event);
  }

  async processQueue() {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      while (this.queue.length > 0) {
        const event = this.queue[0];
        try {
          await this.processEvent(event);
          this.queue.shift();
        } catch (error) {
          console.error("Error processing event:", error);
          // Wait a bit before retrying
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    } finally {
      this.isProcessing = false;
    }
  }

  private async waitForAcceptableGasPrice(
    provider: ethers.Provider = this.provider
  ): Promise<bigint> {
    while (true) {
      const feeData = await provider.getFeeData();
      const gasPrice = feeData.gasPrice;

      if (!gasPrice) {
        throw new Error("Could not get gas price");
      }

      if (gasPrice <= this.MAX_GAS_PRICE) {
        return gasPrice;
      }

      console.log(
        `Current gas price ${this.formatGwei(
          gasPrice
        )} gwei is higher than maximum ${this.formatGwei(
          this.MAX_GAS_PRICE
        )} gwei, waiting ${this.GAS_PRICE_CHECK_DELAY / 1000} seconds...`
      );
      await new Promise((resolve) =>
        setTimeout(resolve, this.GAS_PRICE_CHECK_DELAY)
      );
    }
  }

  private formatGwei(wei: bigint): string {
    return (Number(wei) / 1e9).toFixed(2);
  }

  private async waitForTransactionWithTimeout(
    tx: ethers.TransactionResponse | ethers.ContractTransactionResponse,
    timeoutMs: number
  ): Promise<ethers.TransactionReceipt | null> {
    try {
      // Create a promise that resolves after the timeout
      const timeoutPromise = new Promise<null>((resolve) => {
        setTimeout(() => resolve(null), timeoutMs);
      });

      // Race between the transaction confirmation and the timeout
      const receipt = await Promise.race([tx.wait(), timeoutPromise]);

      return receipt;
    } catch (error) {
      console.error(`Error waiting for transaction confirmation: ${error}`);
      return null;
    }
  }

  private async sendReplacementTransaction(
    event: QueuedPingEvent
  ): Promise<ethers.TransactionReceipt> {
    if (!event.pong_tx_hash || event.pong_tx_nonce == null) {
      throw new Error(
        "Cannot send replacement: missing transaction hash or nonce"
      );
    }

    console.log(
      `Sending replacement for transaction ${event.pong_tx_hash} with nonce ${event.pong_tx_nonce}`
    );

    // Get current block number for tracking
    const currentBlock = await this.provider.getBlock("latest");
    if (!currentBlock) throw new Error("Could not get current block");

    // Wait for acceptable gas price
    const gasPrice = await this.waitForAcceptableGasPrice();
    console.log(
      `Gas price is now ${this.formatGwei(
        gasPrice
      )} gwei, proceeding with transaction`
    );

    const replacementTx = await this.wallet.sendTransaction({
      to: await this.contract.getAddress(),
      data: this.contract.interface.encodeFunctionData("pong", [event.tx_hash]),
      nonce: event.pong_tx_nonce,
      gasPrice: (gasPrice * 120n) / 100n, // 20% more than current gas price for replacement
    });

    // Update DB with replacement transaction and new block number
    await this.db.updateReplacementTransaction(
      event.tx_hash,
      replacementTx.hash,
      currentBlock.number
    );
    console.log(
      `Sent replacement transaction ${replacementTx.hash} with nonce ${event.pong_tx_nonce} at block ${currentBlock.number}`
    );

    // Wait for replacement transaction with timeout and handle recursively if needed
    const { receipt } = await this.waitAndReplace(
      replacementTx,
      event,
      this.TX_CONFIRMATION_TIMEOUT
    );

    // Update DB to mark as processed
    return receipt;
  }

  /**
   * Checks contract logs to see if a pong event has been emitted for a specific ping transaction hash
   * by our wallet address. If found, updates the database and marks the event as processed.
   *
   * @param pingTxHash The transaction hash of the ping event to check for
   * @param startBlock Optional block number to start the search from
   * @returns True if a matching pong event was found and database updated, false otherwise
   */
  async checkPongEventInLogs(
    pingTxHash: string,
    startBlock?: number
  ): Promise<boolean> {
    try {
      // Normalize the input hash for comparison
      const normalizedPingTxHash = pingTxHash.toLowerCase().startsWith("0x")
        ? pingTxHash.toLowerCase()
        : `0x${pingTxHash.toLowerCase()}`;

      console.log(
        `Checking logs for pong event matching ping tx hash ${normalizedPingTxHash} from our address ${this.ourAddress}...`
      );

      // Get current block number
      const currentBlock = await this.provider.getBlock("latest");
      if (!currentBlock) throw new Error("Could not get current block");

      // Use the startBlock parameter if provided, otherwise use a default lookback
      const minBlock = startBlock
        ? startBlock
        : Math.max(0, currentBlock.number - 2000);
      console.log(`Starting search from block ${minBlock}`);

      // Maximum blocks to query in one call (provider limit)
      const maxBlockRange = 500;

      // Define the event to look for according to the ABI
      const eventSignature = "Pong(bytes32)";
      const contractAddress = await this.contract.getAddress();

      // Search logs in chunks and process each chunk immediately
      // Start from the most recent blocks and go backwards
      for (
        let fromBlock = currentBlock.number;
        fromBlock > minBlock;
        fromBlock = fromBlock - maxBlockRange
      ) {
        const toBlock = fromBlock;
        const fromBlockAdjusted = Math.max(
          minBlock,
          fromBlock - maxBlockRange + 1
        );

        console.log(`Searching blocks ${fromBlockAdjusted} to ${toBlock}...`);

        try {
          const filter = {
            address: contractAddress,
            topics: [ethers.id(eventSignature)],
            fromBlock: fromBlockAdjusted,
            toBlock: toBlock,
          };

          const logs = await this.provider.getLogs(filter);

          if (logs.length > 0) {
            console.log(
              `Found ${logs.length} Pong events in blocks ${fromBlockAdjusted}-${toBlock}, checking for matches...`
            );

            // Process logs in reverse order (newest first) for higher chance of finding recent matches
            for (let i = logs.length - 1; i >= 0; i--) {
              const log = logs[i];
              try {
                // Parse the log according to the event structure
                const parsedLog = this.contract.interface.parseLog({
                  topics: log.topics as string[],
                  data: log.data,
                });

                if (parsedLog && parsedLog.args) {
                  const eventTxHash = parsedLog.args.txHash.toLowerCase();
                  if (eventTxHash === normalizedPingTxHash) {
                    // Get the full transaction to check sender
                    const tx = await this.provider.getTransaction(
                      log.transactionHash
                    );
                    if (
                      tx &&
                      tx.from &&
                      tx.from.toLowerCase() === this.ourAddress.toLowerCase()
                    ) {
                      console.log(
                        `Found matching pong event for ping tx ${pingTxHash} in transaction ${log.transactionHash} from our address ${this.ourAddress} at block ${log.blockNumber}`
                      );

                      // Update database with transaction details
                      await this.db.updatePongTransaction(
                        pingTxHash,
                        tx.hash,
                        tx.nonce,
                        log.blockNumber
                      );

                      // Mark the event as processed
                      await this.db.markEventAsProcessed(pingTxHash);
                      console.log(
                        `Marked ${pingTxHash} as processed based on found pong transaction ${tx.hash}`
                      );
                      return true;
                    }
                  }
                }
              } catch (error) {
                console.warn(`Error parsing log: ${error}`);
                // Continue checking other logs in this chunk
              }
            }
          }
        } catch (error) {
          console.warn(
            `Error searching blocks ${fromBlockAdjusted}-${toBlock}: ${error}`
          );
        }
      }
      return false;
    } catch (error) {
      console.error(`Error checking pong event logs: ${error}`);
      return false;
    }
  }

  /**
   * Wait for a transaction to be confirmed with a timeout, and replace it if it times out
   * @param tx The transaction to wait for
   * @param event The event associated with the transaction
   * @param timeout The timeout in milliseconds
   * @returns An object with receipt (if confirmed) and whether the transaction was handled
   */
  private async waitAndReplace(
    tx: ethers.TransactionResponse | ethers.ContractTransactionResponse,
    event: QueuedPingEvent,
    timeout: number
  ): Promise<{ receipt: ethers.TransactionReceipt }> {
    console.log(
      `Waiting for transaction ${tx.hash} to be mined (timeout: ${
        timeout / 1000
      }s)...`
    );
    const receipt = await this.waitForTransactionWithTimeout(tx, timeout);

    if (!receipt) {
      console.log(
        `Transaction ${tx.hash} timed out after ${
          timeout / 1000
        } seconds, sending replacement`
      );
      try {
        event.pong_tx_nonce = tx.nonce;
        const replacementReceipt = await this.sendReplacementTransaction(event);
        return { receipt: replacementReceipt };
      } catch (error) {
        console.error(`Error sending replacement transaction: ${error}`);
        throw error;
      }
    }
    return { receipt };
  }

  /**
   * Process a pending transaction for an event
   * Checks if the transaction is confirmed, stuck, or still pending
   * @param event The event with a pending transaction to process
   * @returns True if the event was fully processed and no further action is needed
   */
  private async processPendingTx(event: QueuedPingEvent): Promise<boolean> {
    if (!event.pong_tx_hash) {
      return false; // No pending transaction to process
    }

    console.log(
      `Found pending transaction ${event.pong_tx_hash} for ping ${event.tx_hash}`
    );

    try {
      // Check if the transaction is already confirmed
      const pendingReceipt = await this.provider.getTransactionReceipt(
        event.pong_tx_hash
      );

      if (pendingReceipt) {
        // Transaction was mined, update as processed
        await this.db.markEventAsProcessed(event.tx_hash);
        console.log(
          `Found and confirmed pending transaction ${event.pong_tx_hash}`
        );
        return true; // Event fully processed
      }

      // Check if transaction is stuck (more than BLOCK_THRESHOLD blocks old)
      if (event.pong_tx_nonce !== null && event.pong_tx_block !== null) {
        const tx = await this.provider.getTransaction(event.pong_tx_hash);
        console.log("TX", tx);
        if (!tx) return false;
        const currentBlock = await this.provider.getBlock("latest");
        if (!currentBlock) throw new Error("Could not get current block");

        const blockDifference = currentBlock.number - event.pong_tx_block;

        if (blockDifference > this.BLOCK_THRESHOLD) {
          console.log(
            `Transaction ${event.pong_tx_hash} is ${blockDifference} blocks old, replacing...`
          );
          try {
            await this.sendReplacementTransaction(event);
          } catch (error) {
            console.error(`Error sending replacement transaction: ${error}`);
            throw error;
          }
          return true; // Event handled with replacement transaction
        } else {
          console.log(
            `Transaction ${event.pong_tx_hash} is only ${blockDifference} blocks old, waiting...`
          );
          const tx = await this.provider.getTransaction(event.pong_tx_hash);
          if (!tx) throw new Error("Transaction not found");

          const timeout = (this.BLOCK_THRESHOLD - blockDifference + 1) * 12000;

          try {
            await this.waitAndReplace(tx, event, timeout);
            // Transaction was confirmed while waiting
            await this.db.markEventAsProcessed(event.tx_hash);
            console.log(
              `Confirmed pending transaction ${event.pong_tx_hash} after waiting`
            );
            return true; // Event fully processed
          } catch (error) {
            console.error(`Error waiting for transaction: ${error}`);
            throw error;
          }
        }
      }

      // If we get here, we have a pending tx_hash but no nonce/block info
      console.log(
        `Pending transaction ${event.pong_tx_hash} doesn't have nonce/block info, treating as new`
      );
      return false; // Treat as unprocessed
    } catch (error) {
      console.log(`Error checking pending transaction: ${error}, will retry`);
      throw error;
    }
  }

  async processEvent(event: QueuedPingEvent) {
    try {
      // Check if a pong event has already been emitted for this ping
      // and update the database if found

      // Check if there's a pending transaction
      if (event.pong_tx_hash) {
        try {
          const handled = await this.processPendingTx(event);
          if (handled) {
                    return; // Transaction was handled, no further action needed
            } else {
                const pongEventFound = await this.checkPongEventInLogs(
                    event.tx_hash,
                    event.block_number
                );
                if (pongEventFound) {
                    return; // Event was found and database was updated
                }
            }
        } catch (error) {
          console.error(`Error processing pending transaction: ${error}`);
          throw error;
        }
      } else {
        if(event.tx_status == "preparing"){
        const pongEventFound = await this.checkPongEventInLogs(
          event.tx_hash,
              event.block_number
            );
            if (pongEventFound) {
              return; // Event was found and database was updated
            }
          }
        } catch (error) {
          console.error(`Error processing pending transaction: ${error}`);
          throw error;
        }
      } else {
        const pongEventFound = await this.checkPongEventInLogs(
          event.tx_hash,
          event.block_number
        );
        if (pongEventFound) {
          return; // Event was found and database was updated
        }
      }

      // Send new pong transaction
      console.log(`Sending new pong transaction for ping ${event.tx_hash}...`);

      const currentBlock = await this.provider.getBlock("latest");
      if (!currentBlock) throw new Error("Could not get current block");

      // Wait for acceptable gas price
      const gasPrice = await this.waitForAcceptableGasPrice();
      console.log(
        `Gas price is now ${this.formatGwei(
          gasPrice
        )} gwei, proceeding with transaction`
      );

      // Override the contract's gas price for this call
      const overrides = { gasPrice };
      const populatedTx = await this.populateTransaction(
        event.tx_hash,
        overrides
      );

      // Store transaction details in the database before sending
      await this.db.updatePingEvent(
        event.tx_hash,
        {
          pong_tx_hash: populatedTx.hash,
          pong_tx_nonce: populatedTx.nonce!,
          pong_tx_block: currentBlock.number,
          tx_status: "preparing",
        }
      );
      console.log(
        `Prepared ping event with transaction ${populatedTx.hash} (nonce: ${populatedTx.nonce}, block: ${currentBlock.number})`
      );

      // Now actually send the transaction
      const tx = await this.wallet.sendTransaction({
        to: populatedTx.to,
        data: populatedTx.data,
        nonce: populatedTx.nonce,
        gasPrice: populatedTx.gasPrice,
        gasLimit: populatedTx.gasLimit,
      });

      console.log(`Sent transaction ${tx.hash}`);

      // Update DB with pending pong transaction, its nonce and block number
      await this.db.updatePingEvent(
        event.tx_hash,
        {
          pong_tx_hash: tx.hash,
          pong_tx_nonce: tx.nonce,
          pong_tx_block: currentBlock.number,
          tx_status: "pending",
        }
      );
      console.log(
        `Updated ping event with pending pong transaction ${
          tx.hash
        } (nonce: ${await tx.nonce}, block: ${currentBlock.number})`
      );

      // Wait for transaction confirmation and replace if needed
      try {
        event.pong_tx_block = currentBlock.number;
        event.pong_tx_nonce = tx.nonce;
        event.pong_tx_hash = tx.hash;
        await this.waitAndReplace(tx, event, this.TX_CONFIRMATION_TIMEOUT);
        // If not handled by replacement and we have a receipt, mark as processed

        await this.db.markEventAsProcessed(event.tx_hash);
        console.log(
          `Processed ping event ${event.tx_hash} with pong ${tx.hash}`
        );
      } catch (error) {
        console.error(`Error waiting for transaction: ${error}`);
        throw error;
      }
    } catch (error) {
      console.error(`Error processing ping event ${event.tx_hash}:`, error);
      throw error; // Propagate error for retry
    }
  }
  /**
   * Create a populated and signed transaction without broadcasting it
   * This allows us to get the hash and other details before sending
   * @param pingTxHash The transaction hash to send a pong for
   * @param overrides Transaction overrides (gas price, etc)
   * @returns The populated and signed transaction
   */
  private async populateTransaction(
    pingTxHash: string,
    overrides: ethers.Overrides = {}
  ): Promise<ethers.TransactionResponse> {
    try {
      // Get the contract address
      const contractAddress = await this.contract.getAddress();

      // Create transaction data manually using the interface
      const data = this.contract.interface.encodeFunctionData("pong", [
        pingTxHash,
      ]);

      // Create the transaction request
      const txRequest: ethers.TransactionRequest = {
        to: contractAddress,
        data,
        ...overrides,
      };

      // Add missing transaction fields
      if (!txRequest.gasLimit) {
        const estimatedGas = await this.provider.estimateGas({
          to: contractAddress,
          data: data,
          from: this.ourAddress,
        });
        txRequest.gasLimit = (estimatedGas * 12n) / 10n; // Add 20% buffer
      }

      if (!txRequest.nonce) {
        txRequest.nonce = await this.wallet.getNonce();
      }

      // Sign the transaction (this gives us the hash without sending)
      const signedTx = await this.wallet.signTransaction(txRequest);

      // Parse the signed transaction to get the hash
      const tx = ethers.Transaction.from(signedTx);

      // Create a transaction response-like object
      return {
        ...txRequest,
        hash: tx.hash,
        nonce: txRequest.nonce,
        wait: async () => {
          throw new Error("Transaction not sent yet");
        },
        confirmations: 0,
        from: this.ourAddress,
      } as unknown as ethers.TransactionResponse;
    } catch (error) {
      console.error("Error populating transaction:", error);
      throw error;
    }
  }
}

export default EventQueue;
