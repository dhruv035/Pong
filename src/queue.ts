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
}

interface PongContract extends ethers.BaseContract {
    pong(txHash: string, overrides?: ethers.Overrides): Promise<ethers.ContractTransactionResponse>;
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

    constructor(db: Database) {
        this.db = db;
        const provider = new ethers.JsonRpcProvider(process.env.SEPOLIA_RPC_URL);
        const wallet = new ethers.Wallet(process.env.PRIVATE_KEY!, provider);
        
        // Initialize contract with signer
        this.contract = new ethers.Contract(
            process.env.CONTRACT_ADDRESS!,
            abi,
            wallet
        ) as unknown as PongContract;
    }

    async initialize() {
        this.queue = await this.db.getUnprocessedEvents();
    }

    async startListening() {
        try {
            const { cleanup } = await this.db.setupNotificationListener(async (payload) => {
                console.log('New ping event received:', payload);
                await this.addEvent(payload);
                await this.processQueue();
            });
            
            this.cleanup = cleanup;
            console.log('Event listener started successfully');
        } catch (error) {
            console.error('Error starting event listener:', error);
            throw error;
        }
    }

    async stopListening() {
        if (this.cleanup) {
            await this.cleanup();
            console.log('Event listener stopped successfully');
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
                console.error('Error processing event:', error);
                    // Wait a bit before retrying
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        } finally {
            this.isProcessing = false;
        }
    }   

    private async waitForAcceptableGasPrice(provider: ethers.Provider): Promise<bigint> {
        while (true) {
            const feeData = await provider.getFeeData();
            const gasPrice = feeData.gasPrice;
            
            if (!gasPrice) {
                throw new Error('Could not get gas price');
            }

            if (gasPrice <= this.MAX_GAS_PRICE) {
                return gasPrice;
            }

            console.log(`Current gas price ${this.formatGwei(gasPrice)} gwei is higher than maximum ${this.formatGwei(this.MAX_GAS_PRICE)} gwei, waiting ${this.GAS_PRICE_CHECK_DELAY/1000} seconds...`);
            await new Promise(resolve => setTimeout(resolve, this.GAS_PRICE_CHECK_DELAY));
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
            const receipt = await Promise.race([
                tx.wait(),
                timeoutPromise
            ]);
            
            return receipt;
        } catch (error) {
            console.error(`Error waiting for transaction confirmation: ${error}`);
            return null;
        }
    }

    private async sendReplacementTransaction(event: QueuedPingEvent): Promise<ethers.TransactionReceipt> {
        console.log(`Sending replacement for transaction ${event.pong_tx_hash} with nonce ${event.pong_tx_nonce}`);
        const wallet = this.contract.runner as ethers.Wallet;
        if (!wallet.provider) throw new Error('No provider available');
        const provider = wallet.provider;

        // Get current block number for tracking
        const currentBlock = await provider.getBlock('latest');
        if (!currentBlock) throw new Error('Could not get current block');
        
        // Wait for acceptable gas price
        const gasPrice = await this.waitForAcceptableGasPrice(provider);
        console.log(`Gas price is now ${this.formatGwei(gasPrice)} gwei, proceeding with transaction`);
        
        try {
            const replacementTx = await wallet.sendTransaction({
                to: await this.contract.getAddress(),
                data: this.contract.interface.encodeFunctionData('pong', [event.tx_hash]),
                nonce: event.pong_tx_nonce!,
                gasPrice: gasPrice * 120n/100n // Double current gas price for replacement
            });
        
        // Update DB with replacement transaction and new block number
        await this.db.updateReplacementTransaction(event.tx_hash, replacementTx.hash, currentBlock.number);
        console.log(`Sent replacement transaction ${replacementTx.hash} with nonce ${event.pong_tx_nonce} at block ${currentBlock.number}`);
        
        // Wait for replacement transaction with timeout
        const receipt = await this.waitForTransactionWithTimeout(replacementTx, this.TX_CONFIRMATION_TIMEOUT);
        
        if (!receipt) {
            console.log(`Replacement transaction ${replacementTx.hash} timed out after ${this.TX_CONFIRMATION_TIMEOUT / 1000} seconds, sending another replacement`);
            // Call ourselves recursively to send another replacement with higher gas
            return this.sendReplacementTransaction(event);
        }
        
            // Update DB to mark as processed
            await this.db.markEventAsProcessed(event.tx_hash);
            console.log(`Processed ping event ${event.tx_hash} with replacement pong ${replacementTx.hash}`);
            return receipt;
        } catch (error) {
            console.error(`Error sending replacement transaction: ${error}`);
            throw error;
        }
    }

    /**
     * Checks contract logs to see if a pong event has been emitted for a specific ping transaction hash
     * by our wallet address
     * @param pingTxHash The transaction hash of the ping event to check for
     * @param startBlock Optional block number to start the search from
     * @returns The transaction details and block number if a matching pong event was found, null otherwise
     */
    async checkPongEventInLogs(pingTxHash: string, startBlock?: number): Promise<{ tx: ethers.TransactionResponse, blockNumber: number } | null> {
        try {
            const wallet = this.contract.runner as ethers.Wallet;
            if (!wallet.provider) throw new Error('No provider available');
            const provider = wallet.provider;
            
            // Get the wallet address we're using to send transactions
            const ourAddress = wallet.address;
            
            // Normalize the input hash for comparison
            const normalizedPingTxHash = pingTxHash.toLowerCase().startsWith('0x') ? 
                pingTxHash.toLowerCase() : `0x${pingTxHash.toLowerCase()}`;
                
            console.log(`Checking logs for pong event matching ping tx hash ${normalizedPingTxHash} from our address ${ourAddress}...`);
            
            // Get current block number
            const currentBlock = await provider.getBlock('latest');
            if (!currentBlock) throw new Error('Could not get current block');
            
            // Use the startBlock parameter if provided, otherwise use a default lookback
            const minBlock = startBlock ? startBlock : Math.max(0, currentBlock.number - 2000);
            console.log(`Starting search from block ${minBlock}`);
            
            // Maximum blocks to query in one call (provider limit)
            const maxBlockRange = 500;
            
            // Define the event to look for according to the ABI
            const eventSignature = "Pong(bytes32)";
            const contractAddress = await this.contract.getAddress();
            
            // Search logs in chunks and process each chunk immediately
            // Start from the most recent blocks and go backwards
            for (let fromBlock = currentBlock.number; fromBlock > minBlock; fromBlock = fromBlock - maxBlockRange) {
                const toBlock = fromBlock;
                const fromBlockAdjusted = Math.max(minBlock, fromBlock - maxBlockRange + 1);
                
                console.log(`Searching blocks ${fromBlockAdjusted} to ${toBlock}...`);
                
                try {
                    const filter = {
                        address: contractAddress,
                        topics: [ethers.id(eventSignature)],
                        fromBlock: fromBlockAdjusted,
                        toBlock: toBlock
                    };
                    
                    const logs = await provider.getLogs(filter);
                    
                    if (logs.length > 0) {
                        console.log(`Found ${logs.length} Pong events in blocks ${fromBlockAdjusted}-${toBlock}, checking for matches...`);
                        
                        // Process logs in reverse order (newest first) for higher chance of finding recent matches
                        for (let i = logs.length - 1; i >= 0; i--) {
                            const log = logs[i];
                            try {
                                // Parse the log according to the event structure
                                const parsedLog = this.contract.interface.parseLog({
                                    topics: log.topics as string[],
                                    data: log.data
                                });
                                
                                if (parsedLog && parsedLog.args) {
                                    // Get the txHash parameter which is the only parameter in the Pong event
                                    const eventTxHash = parsedLog.args.txHash.toLowerCase();
                                    
                                    // Check if the hash matches
                                    if (eventTxHash === normalizedPingTxHash) {
                                        // Get the full transaction to check sender
                                        const tx = await provider.getTransaction(log.transactionHash);
                                        if (tx && tx.from && tx.from.toLowerCase() === ourAddress.toLowerCase()) {
                                            console.log(`Found matching pong event for ping tx ${pingTxHash} in transaction ${log.transactionHash} from our address ${ourAddress} at block ${log.blockNumber}`);
                                            // Return both the transaction and the block number from the log
                                            return { 
                                                tx,
                                                blockNumber: log.blockNumber 
                                            };
                                        } else if (tx) {
                                            console.log(`Found pong event with matching hash but from wrong address: ${tx.from} (we want: ${ourAddress})`);
                                        }
                                    }
                                }
                            } catch (error) {
                                console.warn(`Error parsing log: ${error}`);
                                // Continue checking other logs in this chunk
                            }
                        }
                    } else {
                        console.log(`No Pong events found in blocks ${fromBlockAdjusted}-${toBlock}`);
                    }
                } catch (error) {
                    console.warn(`Error searching blocks ${fromBlockAdjusted}-${toBlock}: ${error}`);
                    // Continue with next chunk even if this one fails
                }
            }
            
            console.log(`No matching pong event found for ping tx ${pingTxHash} from our address ${ourAddress}`);
            return null;
        } catch (error) {
            console.error(`Error checking pong event logs: ${error}`);
            return null;
        }
    }

    async processEvent(event: QueuedPingEvent) {
        try {
            // Check if a pong event has already been emitted for this ping
            const result = await this.checkPongEventInLogs(event.tx_hash, event.block_number);
            
            if (result) {
                const { tx: pongTx, blockNumber } = result;
                console.log(`Found pong transaction ${pongTx.hash} for ping ${event.tx_hash} at block ${blockNumber}`);
                
                // Update our database with transaction details
                await this.db.updatePongTransaction(
                    event.tx_hash,
                    pongTx.hash,
                    await pongTx.nonce, 
                    blockNumber
                );
                
                // Mark the event as processed
                await this.db.markEventAsProcessed(event.tx_hash);
                console.log(`Marked ${event.tx_hash} as processed based on found pong transaction ${pongTx.hash}`);
                return;
            }
            
            // Check if there's a pending transaction
            if (event.pong_tx_hash && !event.processed) {
                console.log(`Found pending transaction ${event.pong_tx_hash} for ping ${event.tx_hash}`);
                try {
                    const wallet = this.contract.runner as ethers.Wallet;
                    if (!wallet.provider) throw new Error('No provider available');
                    const provider = wallet.provider;
                    const pendingReceipt = await provider.getTransactionReceipt(event.pong_tx_hash);
                    
                    if (pendingReceipt) {
                        // Transaction was mined, update as processed
                        await this.db.markEventAsProcessed(event.tx_hash);
                        console.log(`Found and confirmed pending transaction ${event.pong_tx_hash}`);
                        return;
                    }

                    // Check if transaction is stuck (more than BLOCK_THRESHOLD blocks old)
                    if (event.pong_tx_nonce !== null && event.pong_tx_block !== null) {
                        const currentBlock = await provider.getBlock('latest');
                        if (!currentBlock) throw new Error('Could not get current block');
                        
                        const blockDifference = currentBlock.number - event.pong_tx_block;
                        
                        if (blockDifference > this.BLOCK_THRESHOLD) {
                            console.log(`Transaction ${event.pong_tx_hash} is ${blockDifference} blocks old, replacing...`);
                            try {   
                                await this.sendReplacementTransaction(event);
                            } catch (error) {
                                console.error(`Error sending replacement transaction: ${error}`);
                                throw error;
                            }
                            return;
                        } else {
                            console.log(`Transaction ${event.pong_tx_hash} is only ${blockDifference} blocks old, waiting...`);
                            const tx = await provider.getTransaction(event.pong_tx_hash);
                            if (!tx) throw new Error('Transaction not found');
                            const timeout = (this.BLOCK_THRESHOLD - blockDifference +1) * 12000;
                            console.log(`Waiting for transaction ${tx} to be mined (timeout: ${timeout / 1000}s)...`);
                            const receipt = await this.waitForTransactionWithTimeout(tx, timeout);
                            if (!receipt) {
                                console.log(`Transaction ${event.pong_tx_hash} timed out after ${this.TX_CONFIRMATION_TIMEOUT / 1000} seconds, sending replacement`);
                                try {
                                    event.pong_tx_nonce = tx.nonce;
                                    await this.sendReplacementTransaction(event);
                                } catch (error) {
                                    console.error(`Error sending replacement transaction: ${error}`);
                                    throw error;
                                }
                                return;
                            }
                        }
                    }
                } catch (error) {
                    console.log(`Error checking pending transaction: ${error}, will retry`);
                    throw error;
                }
            }

            // Send new pong transaction
            console.log(`Sending new pong transaction for ping ${event.tx_hash}...`);
            const wallet = this.contract.runner as ethers.Wallet;
            if (!wallet.provider) throw new Error('No provider available');
            const provider = wallet.provider;
            
            const currentBlock = await provider.getBlock('latest');
            if (!currentBlock) throw new Error('Could not get current block');
            
            // Wait for acceptable gas price
            const gasPrice = await this.waitForAcceptableGasPrice(provider);
            console.log(`Gas price is now ${this.formatGwei(gasPrice)} gwei, proceeding with transaction`);
            
            // Override the contract's gas price for this call
            const overrides = { gasPrice };
            const tx = await this.contract.pong(event.tx_hash, overrides);
            
            // Update DB with pending pong transaction, its nonce and block number
            await this.db.updatePongTransaction(event.tx_hash, tx.hash, tx.nonce, currentBlock.number);
            console.log(`Updated ping event with pending pong transaction ${tx.hash} (nonce: ${await tx.nonce}, block: ${currentBlock.number})`);

            // Wait for transaction confirmation with timeout
            console.log(`Waiting for pong transaction to be mined (timeout: ${this.TX_CONFIRMATION_TIMEOUT / 1000}s)...`);
            const receipt = await this.waitForTransactionWithTimeout(tx, this.TX_CONFIRMATION_TIMEOUT);
            
            if (!receipt) {
                console.log(`Transaction ${tx.hash} timed out after ${this.TX_CONFIRMATION_TIMEOUT / 1000} seconds, sending replacement`);
                // Send replacement transaction
                event.pong_tx_nonce = tx.nonce;
                return this.sendReplacementTransaction(event);
            }

            // Update DB to mark as processed after confirmation
            await this.db.markEventAsProcessed(event.tx_hash);
            console.log(`Processed ping event ${event.tx_hash} with pong ${tx.hash}`);
        } catch (error) {
            console.error(`Error processing ping event ${event.tx_hash}:`, error);
            throw error; // Propagate error for retry
        }
    }   
}

export default EventQueue;