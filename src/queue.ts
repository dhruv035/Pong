import { ethers } from "ethers";
import { abi } from "./abi";
import { Database } from "./db";

export type QueuedPingEvent = {
  tx_hash: string;
  processed: boolean;
  pong_tx_nonce: number;
  block_number: number;
};

export type PongTransaction = {
  tx_hash: string;
  replacement_hash: string | null;
  nonce: number;
  block_number: number | null;
  ping_hash: string;
  status: string;
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
  private BLOCK_THRESHOLD = 0; // Number of blocks to wait before replacement
  private wallet: ethers.Wallet;
  private provider: ethers.Provider;
  private ourAddress: string;
  private blockingNonce: number = -1;
  private lastBlock: number = -1;

  constructor() {
    this.db = new Database();
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
    console.log("Processing queue, queue length:", this.isProcessing);
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
  async sendPong(
    event: QueuedPingEvent,
    isReplacement: boolean = false,
    isPrepared: boolean = false
  ) {
    const { populatedTx, hash } = await this.populateTransaction(
      event.tx_hash,
      {
        nonce: event.pong_tx_nonce,
      },
      isReplacement
    );
    if (!populatedTx.nonce) {
      throw new Error("Nonce is required");
    }
    let currentBlock: ethers.Block | undefined;
    do {
      const block = await this.provider.getBlock("latest");
      if (block) {
        currentBlock = block;
      } else {
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    } while (!currentBlock);
    if (!currentBlock) throw new Error("Could not get current block");
    if (this.blockingNonce > populatedTx.nonce) {
      console.log("This transaction is already mined, skipping");
      return;
    }
    await this.db.preparePongTransaction(
      event.tx_hash,
      hash,
      populatedTx.nonce,
      currentBlock.number,
      isReplacement,
      isPrepared
    );

    const tx = await this.wallet
      .sendTransaction(populatedTx)
      .then((tx) => {
        if (this.blockingNonce === -1 || this.blockingNonce >= tx.nonce) {
          this.blockingNonce = tx.nonce;
          this.lastBlock = currentBlock.number;
        }
        return tx;
      })
      .catch((error) => {
        const err = error as ethers.EthersError;
        if (err.shortMessage === "replacement fee too low") {
          console.log("Replacement fee too low, skipping");
          return undefined;
        }
        throw error;
      });

    const updates: any = {
      status: "pending",
    };
    if (isReplacement) {
      updates.tx_hash = hash;
      updates.replacement_hash = null;
    }
    await this.db.updatePongTransaction(populatedTx.nonce, updates);
    return tx;
  }
  async updateBlockingNonce(blockNumber: number, nonce: number) {
    if (nonce >= this.blockingNonce) {
      this.blockingNonce = nonce + 1;
      this.lastBlock = blockNumber;
    }
  }
  blockWatcher() {
    this.provider.on("block", async (blockNumber) => {
      console.log("blockWatcher", blockNumber, this.lastBlock);
      if (this.lastBlock === -1) {
        return;
      }
      const nonce = await this.provider.getTransactionCount(this.ourAddress);
      console.log("nonce, blockingNonce", nonce, this.blockingNonce);
      if (this.blockingNonce > nonce) {
        console.log(
          "Blocking nonce is greater than the current nonce, skipping"
        );
        return;
      }
      if (blockNumber - this.lastBlock > this.BLOCK_THRESHOLD) {
        const pingEvent = await this.db.getPingEvent(this.blockingNonce);
        if (!pingEvent) {
          this.blockingNonce = -1;
          this.lastBlock = -1;
          return;
        }

        const dbTx = await this.db.getPongTransaction(this.blockingNonce);
        let tx: ethers.TransactionResponse | undefined;
        if (dbTx) {
          tx = await this.sendPong(pingEvent, true);
        } else {
          tx = await this.sendPong(pingEvent);
        }

        if (!tx) {
          console.log("Transaction was processed, skipping");
          return;
        }
        console.log("tx", tx);
        tx.wait()
          .then(async (receipt) => {
            if (!receipt || !receipt.blockNumber) {
              return;
            }
            await this.db.confirmTransaction(tx.hash, tx.nonce);
            if (tx.nonce === this.blockingNonce) {
              await this.updateBlockingNonce(receipt.blockNumber, tx.nonce);
            }
          })
          .catch((error) => {
            const err = error as ethers.EthersError;
            if (err.shortMessage === "transaction was replaced") {
              console.log("Transaction was replaced");
            } else {
              console.error(
                `Error waiting for transaction: ${err.shortMessage}`
              );
              throw error;
            }
          });
      }
    });
  }

  async recoverTransaction(dbTx: PongTransaction, event: QueuedPingEvent) {
    const currentBlock = await this.provider.getBlock("latest");
    if (!currentBlock) throw new Error("Could not get current block");
    if (dbTx.status === "preparing" || dbTx.status === "pending") {
      console.log("RECOVERING TRANSACTION, status preparing or pending");

      const tx = await this.provider.getTransaction(dbTx.tx_hash);
      if (!tx) {
        const newTx = await this.sendPong(event, true, true);
        if (!newTx) throw new Error("Transaction not found");
        newTx
          .wait()
          .then(async (receipt) => {
            if (!receipt || !receipt.blockNumber) {
              return;
            }
            await this.db.confirmTransaction(newTx.hash, newTx.nonce);
            await this.updateBlockingNonce(receipt.blockNumber, newTx.nonce);
          })
          .catch((error) => {
            const err = error as ethers.EthersError;
            if (err.shortMessage === "transaction was replaced") {
              console.log("Transaction was replaced");
            } else {
              console.error(
                `Error waiting for transaction: ${err.shortMessage}`
              );
              throw error;
            }
          });
      } else if (!tx.blockNumber) {
        if (this.blockingNonce === -1 || this.blockingNonce > tx.nonce) {
          this.blockingNonce = tx.nonce;
          this.lastBlock = dbTx.block_number!;
        }
        await this.db.updatePongTransaction(dbTx.nonce, {
          status: "pending",
        });

        tx.wait()
          .then(async (receipt) => {
            if (!receipt || !receipt.blockNumber) {
              return;
            }
            await this.db.confirmTransaction(tx.hash, tx.nonce);
            await this.updateBlockingNonce(receipt.blockNumber, tx.nonce);
          })
          .catch((error) => {
            const err = error as ethers.EthersError;
            if (err.shortMessage === "transaction was replaced") {
              console.log("Transaction was replaced");
            } else {
              console.error(
                `Error waiting for transaction: ${err.shortMessage}`
              );
              throw error;
            }
          });
      } else {
        await this.db.confirmTransaction(tx.hash, tx.nonce);
        await this.updateBlockingNonce(tx.blockNumber, tx.nonce);
      }
    } else if (dbTx.status === "replacing") {
      const tx = await this.provider.getTransaction(dbTx.tx_hash);
      const replacementTx = await this.provider.getTransaction(
        dbTx.replacement_hash!
      );
      if (tx?.blockNumber) {
        await this.db.confirmTransaction(tx.hash, tx.nonce);
        await this.updateBlockingNonce(tx.blockNumber, tx.nonce);
      } else if (replacementTx) {
        if (replacementTx.blockNumber) {
          await this.db.confirmTransaction(
            replacementTx.hash,
            replacementTx.nonce
          );
          await this.updateBlockingNonce(
            replacementTx.blockNumber,
            replacementTx.nonce
          );
        } else {
          await this.db.updatePongTransaction(dbTx.nonce, {
            status: "pending",
            replacement_hash: null,
            tx_hash: replacementTx.hash,
          });
          if (
            this.blockingNonce === -1 ||
            this.blockingNonce >= replacementTx.nonce
          ) {
            this.blockingNonce = replacementTx.nonce;
            this.lastBlock = dbTx.block_number!;
          }

          replacementTx
            .wait()
            .then(async (receipt) => {
              if (!receipt || !receipt.blockNumber) {
                return;
              }
              await this.db.confirmTransaction(
                replacementTx.hash,
                replacementTx.nonce
              );
              await this.updateBlockingNonce(
                receipt.blockNumber,
                replacementTx.nonce
              );
            })
            .catch((error) => {
              const err = error as ethers.EthersError;
              if (err.shortMessage === "transaction was replaced") {
                console.log("Transaction was replaced");
              } else {
                console.error(
                  `Error waiting for transaction: ${err.shortMessage}`
                );
                throw error;
              }
            });
        }
      } else {
        console.log("TX not found, sending new transaction");
        const tx = await this.sendPong(event, true, true);
        if (!tx) throw new Error("Transaction not found");
        tx.wait()
          .then(async (receipt) => {
            if (!receipt || !receipt.blockNumber) {
              return;
            }
            await this.db.confirmTransaction(tx.hash, tx.nonce);
            this.updateBlockingNonce(receipt.blockNumber, tx.nonce);
          })
          .catch((error) => {
            const err = error as ethers.EthersError;
            if (err.shortMessage === "transaction was replaced") {
              console.log("Transaction was replaced");
            } else {
              console.error(
                `Error waiting for transaction: ${err.shortMessage}`
              );
              throw error;
            }
          });
      }
    }
  }

  async processEvent(event: QueuedPingEvent) {
    try {
      console.log("event.pong_tx_nonce", event.pong_tx_nonce);
      // Check if there's a pending transaction
      const dbTx = await this.db.getPongTransaction(event.pong_tx_nonce);
      if (dbTx) {
        console.log("RECOVERING TRANSACTION");
        await this.recoverTransaction(dbTx, event);
      } else {
        const tx = await this.sendPong(event);
        if (!tx) throw new Error("Transaction not found");
        tx.wait()
          .then(async (receipt) => {
            if (!receipt || !receipt.blockNumber) {
              return;
            }
            await this.db.confirmTransaction(tx.hash, tx.nonce);
            this.updateBlockingNonce(receipt.blockNumber, tx.nonce);
          })
          .catch((error) => {
            const err = error as ethers.EthersError;
            if (err.shortMessage === "transaction was replaced") {
              console.log("Transaction was replaced");
            } else {
              console.error(
                `Error waiting for transaction: ${err.shortMessage}`
              );
              throw error;
            }
          });
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
    overrides: ethers.Overrides = {},
    isReplacement: boolean = false
  ): Promise<{ populatedTx: ethers.TransactionLike; hash: string }> {
    try {
      // Get the contract address
      const contractAddress = await this.contract.getAddress();

      // Create transaction data manually using the interface
      const data = this.contract.interface.encodeFunctionData("pong", [
        pingTxHash,
      ]);

      // Get fee data for EIP-1559
      const feeData = await this.provider.getFeeData();
      if (!feeData.maxFeePerGas || !feeData.maxPriorityFeePerGas) {
        throw new Error("Could not get fee data for EIP-1559");
      }

      // Create the transaction request with EIP-1559 fields
      const txRequest: ethers.TransactionRequest = {
        to: contractAddress,
        data,
        type: 2, // EIP-1559
        maxFeePerGas: isReplacement
          ? (feeData.maxFeePerGas * 12n) / 10n
          : feeData.maxFeePerGas,
        maxPriorityFeePerGas: isReplacement
          ? (feeData.maxPriorityFeePerGas * 12n) / 10n
          : feeData.maxPriorityFeePerGas,
        chainId: (await this.provider.getNetwork()).chainId,
        nonce: overrides.nonce,
      };

      // Add missing transaction fields
      if (!txRequest.gasLimit) {
        const estimatedGas = await this.provider.estimateGas({
          to: contractAddress,
          data: data,
          from: this.ourAddress,
        });
        txRequest.gasLimit = estimatedGas; // Add 20% buffer
      }

      if (!txRequest.nonce) {
        throw new Error("Nonce is required");
      }

      // Let the wallet populate the transaction to match exactly what will be sent
      const populatedTx = await this.wallet.populateTransaction(txRequest);

      // Get the actual transaction hash that will match the sent transaction
      const signedTx = await this.wallet.signTransaction(populatedTx);
      const tx = ethers.Transaction.from(signedTx);

      if (tx.hash) {
        return {
          populatedTx,
          hash: tx.hash,
        };
      } else {
        throw new Error("Transaction hash is null");
      }
    } catch (error) {
      console.error("Error populating transaction:", error);
      throw error;
    }
  }
}

export default EventQueue;
