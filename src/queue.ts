import { ethers } from "ethers";
import { Database } from "./db";
import { abi } from "./abi";
import {
  getContract,
  PublicClient,
  keccak256,
  WalletClient,
  createWalletClient,
  webSocket,
  createPublicClient,
  TransactionReceipt,
  encodeFunctionData,
  TransactionSerializable,
  parseTransaction,
  serializeTransaction,
  Transaction,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { sepolia } from "viem/chains";

export type QueuedPingEvent = {
  tx_hash: string;
  processed: boolean;
  pong_tx_nonce: number | null;
  block_number: number;
};

export type PongTransaction = {
  tx_hash: string;
  replacement_hash: string | null;
  nonce: number | null;
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
  private publicClient: PublicClient;
  private walletClient: WalletClient;
  private db: Database;
  private cleanup: (() => Promise<void>) | null = null;
  private BLOCK_THRESHOLD = 5; // Number of blocks to wait before replacement
  private readonly MAX_GAS_PRICE = 250n * 1000000000n; // 250 gwei
  private readonly GAS_PRICE_CHECK_DELAY = 12000; // 12 seconds in milliseconds
  private readonly TX_CONFIRMATION_TIMEOUT = 60000; // 60 seconds timeout for transaction confirmation

  constructor(db: Database) {
    this.db = db;
    this.publicClient = createPublicClient({
      chain: sepolia,
      transport: webSocket(process.env.SEPOLIA_RPC_URL!),
    });
    this.walletClient = createWalletClient({
      account: privateKeyToAccount(process.env.PRIVATE_KEY! as `0x${string}`),
      transport: webSocket(process.env.SEPOLIA_RPC_URL!),
    });
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

  private async waitForAcceptableGasPrice(): Promise<bigint> {
    while (true) {
      const { maxFeePerGas } = await this.publicClient.estimateFeesPerGas();
      const gasPrice = maxFeePerGas;

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
    txHash: string,
    timeoutMs: number
  ): Promise<TransactionReceipt | null> {
    try {
      // Create a promise that resolves after the timeout
      const timeoutPromise = new Promise<null>((resolve) => {
        setTimeout(() => resolve(null), timeoutMs);
      });

      // Race between the transaction confirmation and the timeout
      const receipt = await Promise.race([
        this.publicClient.waitForTransactionReceipt({
          hash: txHash as `0x${string}`,
        }),
        timeoutPromise,
      ]);

      return receipt;
    } catch (error) {
      console.error(`Error waiting for transaction confirmation: ${error}`);
      return null;
    }
  }

  private async waitAndReplaceNew(
    event: QueuedPingEvent,
    isReplacement: boolean = false,
    isPrepared: boolean = false
  ) {
    let shouldRetry = isReplacement;
    console.log(
      "waiting and replacing new, event, isReplacement",
      event,
      isReplacement
    );
    do {
      const { receipt, replace } = await this.sendPongTransaction(
        event,
        shouldRetry,
        isPrepared
      );
      if (receipt) {
        return { receipt, replace: false };
      }
      shouldRetry = replace;
    } while (shouldRetry);
  }
  private async sendPongTransaction(
    event: QueuedPingEvent,
    isReplacement: boolean,
    isPrepared: boolean
  ): Promise<{ receipt: TransactionReceipt | null; replace: boolean }> {
    const currentBlock = Number(await this.publicClient.getBlockNumber());
    if (!currentBlock) throw new Error("Could not get current block");

    const contract = getContract({
      address: process.env.CONTRACT_ADDRESS! as `0x${string}`,
      abi,
      client: this.walletClient,
    });
    const gasPrice = await this.waitForAcceptableGasPrice();
    const overrides = {
      gasPrice: isReplacement ? (gasPrice * 12n) / 10n : gasPrice,
    };
    const serializedTx = await this.populateTransaction(event.tx_hash, overrides);

    const signedTx = parseTransaction(serializedTx);
    const txHash = keccak256(serializedTx);

    console.log("txHash", txHash);

    const dbTx = await this.db.getPongTransaction(event.pong_tx_nonce!);
    if (!signedTx.nonce) {
      throw new Error("Transaction nonce is undefined");
    }
    if (signedTx.nonce !== event.pong_tx_nonce) {
      if (dbTx) {
        let tx: TransactionReceipt | undefined;
        try {
          tx = await this.publicClient.getTransactionReceipt({
            hash: dbTx.tx_hash as `0x${string}`,
          });
        } catch (error) {
          if (  
            error instanceof Error &&
            error.message.includes("could not be found")
          ) {
            console.log(`Transaction receipt not found: ${error}`);
          } else {
            throw error;
          }
        }
        if (tx) {
          console.log("Transaction was confirmed before replacement");
          await this.db.confirmTransaction(
            tx.transactionHash,
            event.pong_tx_nonce!
          );
          return { receipt: tx, replace: false };
        }
      }
    }

    await this.db.preparePongTransaction(
      event.tx_hash,
      txHash,
      signedTx.nonce,
      currentBlock,
      isReplacement,
      isPrepared
    );
    let tx: TransactionReceipt | undefined;

    try {
      // Send the exact populated transaction
      const txHash = await this.walletClient.sendRawTransaction({
        serializedTransaction: serializedTx,
      });
    } catch (error) {
      console.error(`Error sending transaction: ${error}`);
      return { receipt: null, replace: true };
    }
    console.log("sent tx", tx);
    await this.db.updatePongTransaction(
      signedTx.nonce,
      isReplacement
        ? {
            tx_hash: txHash,
            replacement_hash: null,
          }
        : {
            status: "pending",
          }
    );
    const receipt = await this.waitForTransactionWithTimeout(
      txHash,
      this.TX_CONFIRMATION_TIMEOUT
    );
    if (receipt) {
      await this.db.confirmTransaction(receipt.transactionHash, signedTx.nonce);
      return { receipt, replace: false };
    } else {
      return { receipt: null, replace: true };
    }
  }
  async processTxHash(event: QueuedPingEvent, nonce: number) {
    try {
      const dbTx = await this.db.getPongTransaction(nonce);
      const currentBlock = await this.publicClient.getBlockNumber();
      if (!currentBlock) throw new Error("Could not get current block");
      if (!dbTx) {
        throw new Error("Transaction not found");
      }

      let txReceipt: TransactionReceipt | undefined;
      try {
        txReceipt = await this.publicClient.getTransactionReceipt({
          hash: dbTx.tx_hash as `0x${string}`,
        });
      } catch (error) {
        if (
          error instanceof Error &&
          error.message.includes("could not be found")
        ) {
          console.log(`Transaction receipt not found: ${error}`);
        } else {
          throw error;
        }
      }
      if (!txReceipt) {
        console.log("Transaction receipt not found, checking status");
        if (dbTx.status === "replacing") {
          let replacementTx: Transaction | undefined;
          try {
            replacementTx = await this.publicClient.getTransaction({
              hash: dbTx.replacement_hash! as `0x${string}`,
            });
          } catch (error) {
            if (
              error instanceof Error &&
              error.message.includes("could not be found")
            ) {
              console.log(`Error getting replacement transaction: ${error}`);
            } else {
              throw error;
            }
          }
          if (!replacementTx) {
            const nonce = Number(
              await this.publicClient.getTransactionCount({
                address: this.walletClient.account!.address,
              })
            );
            console.log("nonce", nonce);
            console.log("dbTx.nonce", dbTx.nonce);
            if (nonce !== Number(dbTx.nonce)) {
              throw new Error("Transaction has a different nonce, aborting");
            }
            try {
              await this.waitAndReplaceNew(event, true, false);
            } catch (error) {
              console.error(`Error waiting for transaction: ${error}`);
              throw error;
            }
          } else if (replacementTx.blockNumber) {
            await this.db.confirmTransaction(
              replacementTx.hash,
              replacementTx.nonce
            );
          } else {
            console.log(
              `Replacement transaction ${dbTx.replacement_hash} is still pending, waiting...`
            );
            const blockDiff = Number(currentBlock) - Number(dbTx.block_number);
            await this.processPendingTransaction(
              replacementTx.hash,
              event,
              blockDiff
            );
          }
        } else {
          if (dbTx.status === "preparing") {
            console.log(
              `Transaction ${dbTx.tx_hash} was not sent, preparing new transaction...`
            );
          } else if (dbTx.status === "pending") {
            console.log(
              `Transaction ${dbTx.tx_hash} was dropped, preparing new transaction...`
            );
          }
          const nonce = Number(
            await this.publicClient.getTransactionCount({
              address: this.walletClient.account!.address,
            })
          );
          console.log("nonce", nonce);
          console.log("dbTx.nonce", dbTx.nonce);
          if (nonce !== Number(dbTx.nonce)) {
            throw new Error("Transaction has a different nonce, aborting");
          }
          try {
            await this.waitAndReplaceNew(event, false, true);
          } catch (error) {
            console.error(`Error waiting for transaction: ${error}`);
            throw error;
          }
        }
      } else if (txReceipt?.blockNumber) {
        console.log("TX   ", txReceipt);
        console.log(
          `Transaction ${dbTx.tx_hash} was mined, updating database...`
        );
        const tx = await this.publicClient.getTransaction({
          hash: dbTx.tx_hash as `0x${string}`,
        });
        console.log("TX hash", tx.hash);
        console.log("TX nonce", tx.nonce);
        await this.db.confirmTransaction(tx.hash, tx.nonce);
        console.log("TX updated", event.pong_tx_nonce);
      } else {
        console.log(`Transaction ${dbTx.tx_hash} is still pending, waiting...`);
        const blockDiff = Number(currentBlock) - dbTx.block_number!;
        await this.processPendingTransaction(
          txReceipt!.transactionHash,
          event,
          blockDiff
        );
      }
    } catch (error) {
      console.error(`Error processing transaction: ${error}`);
      throw error;
    }
  }

  async processPendingTransaction(
    txHash: string,
    event: QueuedPingEvent,
    blockDiff: number
  ) {
    console.log("processing pending transaction", txHash, event, blockDiff);
    const currentBlock = await this.publicClient.getBlockNumber();
    if (!currentBlock) throw new Error("Could not get current block");
    const receipt = await this.waitForTransactionWithTimeout(
      txHash,
      blockDiff < this.BLOCK_THRESHOLD
        ? 12000 * (this.BLOCK_THRESHOLD - blockDiff)
        : 0
    );
    if (receipt) {
      const tx = await this.publicClient.getTransaction({
        hash: txHash as `0x${string}`,
      });
      await this.db.confirmTransaction(tx.hash, tx.nonce);
    } else {
      console.log(`Transaction ${txHash} is stuck, sending replacement...`);
      await this.waitAndReplaceNew(event, true);
    }
  }

  async processEvent(event: QueuedPingEvent) {
    try {
      console.log("event.pong_tx_nonce", event.pong_tx_nonce);
      // Check if there's a pending transaction
      if (event.pong_tx_nonce) {
        try {
          await this.processTxHash(event, event.pong_tx_nonce);
        } catch (error) {
          console.error(`Error processing transaction: ${error}`);
          throw error;
        }
      } else {
        // Send new pong transaction
        await this.waitAndReplaceNew(event, false);
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
    overrides: { gasPrice?: bigint } = {}
  ): Promise<`0x02${string}`> {
    try {
      // Get the contract address
      const contractAddress = process.env.CONTRACT_ADDRESS! as `0x${string}`;

      // Encode function data for 'pong' using viem
      const data = encodeFunctionData({
        abi,
        functionName: "pong",
        args: [pingTxHash as `0x${string}`],
      });

      // Get nonce from public client
      const nonce = Number(
        await this.publicClient.getTransactionCount({
          address: this.walletClient.account!.address,
        })
      );

      // Get latest block for fee estimation
      const block = await this.publicClient.getBlock();

      // Calculate fees based on overrides or block data
      const maxFeePerGas = overrides.gasPrice
        ? overrides.gasPrice
        : (block.baseFeePerGas || 10000000000n) * 2n;

      const maxPriorityFeePerGas = overrides.gasPrice
        ? overrides.gasPrice / 2n
        : maxFeePerGas / 2n;

      // Get gas estimate
      const gasEstimate = await this.publicClient.estimateGas({
        account: this.walletClient.account!.address,
        to: contractAddress,
        data,
        value: 0n,
      });

      // Add 20% buffer to gas estimate
      const gas = (gasEstimate * 12n) / 10n;

      console.log("gasLimit", gas);
      // Get chain id
      const chainId = this.publicClient.chain?.id || 11155111; // Default to Sepolia if not available

      // Prepare transaction request
      const txRequest = {
        to: contractAddress,
        data,
        nonce,
        maxFeePerGas,
        maxPriorityFeePerGas,
        gas,
        type: "eip1559" as const,
        chainId,
        value: 0n,
      };

      // Sign transaction to get hash
      const signedTx = await this.walletClient.signTransaction({
        chain: this.publicClient.chain,
        account: this.walletClient.account!,
        ...txRequest,
      });
      // Return transaction response-like object with viem types
      return signedTx;
    } catch (error) {
      console.error("Error populating transaction:", error);
      throw error;
    }
  }
}

export default EventQueue;
