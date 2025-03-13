import { ethers } from "ethers";
import { abi } from "./abi";
import { Database } from "./db";

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

  private async waitAndReplaceNew(
    event: QueuedPingEvent,
    isReplacement: boolean = false,
    isPrepared: boolean = false
  ) {
    let shouldRetry = isReplacement;
    console.log("waiting and replacing new, event, isReplacement", event, isReplacement);
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
  ): Promise<{ receipt: ethers.TransactionReceipt | null; replace: boolean }> {
    const currentBlock = await this.provider.getBlock("latest");
    if (!currentBlock) throw new Error("Could not get current block");

    const gasPrice = await this.waitForAcceptableGasPrice();
    const overrides = { gasPrice: isReplacement ? gasPrice*12n/10n : gasPrice*8n/10n };
    const populatedTx = await this.populateTransaction(
      event.tx_hash,
      overrides
    );

    console.log("populatedTx", populatedTx);

    await this.db.preparePongTransaction(
      event.tx_hash,
      populatedTx.hash,
      populatedTx.nonce,
      currentBlock.number,
      isReplacement,
      isPrepared
    );
    let tx: ethers.TransactionResponse | ethers.ContractTransactionResponse;
    try {
      // Send the exact populated transaction
      tx = await this.wallet.sendTransaction(populatedTx);
    } catch (error) {
      console.error(`Error sending transaction: ${error}`);
      return { receipt: null, replace: true };
    }
    console.log("sent tx", tx);
    await this.db.updatePongTransaction(
      populatedTx.nonce,
      isReplacement
        ? {
            tx_hash: populatedTx.hash,
            replacement_hash: null,
          }
        : {
            status: "pending",
          }
    );
    const receipt = await this.waitForTransactionWithTimeout(
      tx,
      this.TX_CONFIRMATION_TIMEOUT
    );
    if (receipt) {
      await this.db.confirmTransaction(tx.hash, tx.nonce);
      return { receipt, replace: false };
    } else {
      return { receipt: null, replace: true };
    }
  }
  async processTxHash(event: QueuedPingEvent, nonce: number) {
    try {
      const dbTx = await this.db.getPongTransaction(nonce);
      const currentBlock = await this.provider.getBlock("latest");
      if (!currentBlock) throw new Error("Could not get current block");
      if (!dbTx) {
        throw new Error("Transaction not found");
      }
      const tx = await this.provider.getTransaction(dbTx.tx_hash);
      if (!tx) {
        if (dbTx.status === "replacing") {
          const replacementTx = await this.provider.getTransaction(
            dbTx.replacement_hash!
          );
          if (!replacementTx) {
            const nonce = await this.wallet.getNonce();
            console.log("nonce", nonce);
            console.log("dbTx.nonce", dbTx.nonce);
            if (nonce !== dbTx.nonce) {
              throw new Error("Transaction has a different nonce, aborting");
            }
            try {
              await this.waitAndReplaceNew(event);
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
            const blockDiff = currentBlock.number - dbTx.block_number!;
            await this.processPendingTransaction(replacementTx, event,blockDiff);
          }
        }

       else {  if (dbTx.status === "preparing") {
          console.log(
            `Transaction ${dbTx.tx_hash} was not sent, preparing new transaction...`
          );
        } else if (dbTx.status === "pending") {
          console.log(
            `Transaction ${dbTx.tx_hash} was dropped, preparing new transaction...`
          );
        }
        const nonce = await this.wallet.getNonce();
        console.log("nonce", nonce);
        console.log("dbTx.nonce", dbTx.nonce);
        if (nonce !== Number(dbTx.nonce)) {
          throw new Error("Transaction has a different nonce, aborting");
        }
        try {
          await this.waitAndReplaceNew(event,false,true);
        } catch (error) {
          console.error(`Error waiting for transaction: ${error}`);
          throw error;
        }}
      } else if (tx.blockNumber) {
        console.log("TX   ", tx);
        console.log(
          `Transaction ${dbTx.tx_hash} was mined, updating database...`
        );
        console.log("TX hash", tx.hash);
        console.log("TX nonce", tx.nonce);
        await this.db.confirmTransaction(tx.hash, tx.nonce);
        console.log("TX updated", event.pong_tx_nonce);
      } else {
        console.log(`Transaction ${dbTx.tx_hash} is still pending, waiting...`);
        const blockDiff = currentBlock.number - dbTx.block_number!;
        await this.processPendingTransaction(tx, event,blockDiff);
      }
    } catch (error) {
      console.error(`Error processing transaction: ${error}`);
      throw error;
    }
  }

  async processPendingTransaction(
    tx: ethers.TransactionResponse | ethers.ContractTransactionResponse,
    event: QueuedPingEvent,
    blockDiff: number
  ) {
    console.log("processing pending transaction", tx, event, blockDiff);
    const currentBlock = await this.provider.getBlock("latest");
    if (!currentBlock) throw new Error("Could not get current block");
    const receipt = await this.waitForTransactionWithTimeout(
      tx,
      blockDiff<this.BLOCK_THRESHOLD ? 12000*(this.BLOCK_THRESHOLD-blockDiff) : 0
    );
    if (receipt) {
      await this.db.confirmTransaction(tx.hash, tx.nonce);
    } else {
      console.log(`Transaction ${tx.hash} is stuck, sending replacement...`);
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
    overrides: ethers.Overrides = {}
  ): Promise<ethers.TransactionResponse> {
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
        maxFeePerGas: overrides.gasPrice || feeData.maxFeePerGas,
        maxPriorityFeePerGas: overrides.gasPrice || feeData.maxPriorityFeePerGas,
        chainId: (await this.provider.getNetwork()).chainId
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

      // Let the wallet populate the transaction to match exactly what will be sent
      const populatedTx = await this.wallet.populateTransaction(txRequest);
      
      // Get the actual transaction hash that will match the sent transaction
      const signedTx = await this.wallet.signTransaction(populatedTx);
      const tx = ethers.Transaction.from(signedTx);

      return {
        ...populatedTx,
        hash: tx.hash,
        wait: async () => {
          throw new Error("Transaction not sent yet");
        },
        confirmations: 0,
      } as unknown as ethers.TransactionResponse;
    } catch (error) {
      console.error("Error populating transaction:", error);
      throw error;
    }
  }
}

export default EventQueue;
