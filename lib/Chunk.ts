// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ChunkData = Record<string, any>;
export type ChunkType = "data" | "error" | "metadata";

export type ChunkOptions = DataChunkOptions | ErrorChunkOptions | MetadataChunkOptions;

export interface DataChunkOptions {
    type: "data";
    data: ChunkData;
}

export interface ErrorChunkOptions {
    type: "error";
    data: ChunkData;
    error: Error;
}

export interface MetadataChunkOptions {
    type: "metadata";
    // metadata: Record<string, any>;
}

/**
 * A quantum of data that will flow through the stream. Usually a DataChunk, but may be an ErrorChunk if
 * there was an error processing the data or a MetadataChunk if it's not data and not an error.
 */
export abstract class Chunk {
    type: ChunkType;

    /**
     * Creates a Chunk. Only used by the static method Chunk.create()
     *
     * @param type - The type of the Chunk
     * @param _opt - Options for the Chunk. Not currently used.
     */
    constructor(type: ChunkType, _opt: ChunkOptions) {
        this.type = type;
    }

    /**
     * Indicates whether the Chunk is a DataChunk. Also acts as a TypeScript type guard.
     *
     * @returns True if the Chunk is a DataChunk, false otherwise
     */
    isData(): this is DataChunk {
        return this.type === "data";
    }

    /**
     * Indicates whether the Chunk is a ErrorChunk. Also acts as a TypeScript type guard.
     *
     * @returns True if the Chunk is a ErrorChunk, false otherwise
     */
    isError(): this is ErrorChunk {
        return this.type === "error";
    }

    /**
     * Indicates whether the Chunk is a MetadataChunk. Also acts as a TypeScript type guard.
     *
     * @returns True if the Chunk is a MetadataChunk, false otherwise
     */
    isMetadata(): this is MetadataChunk {
        return this.type === "metadata";
    }

    /**
     * Creates a new Chunk with the specified options
     *
     * @param opts - Optiones for creating the Chunk
     * @returns The newly created Chunk
     */
    static create(opts: ChunkOptions = {type: "data", data: {}}): Chunk {
        if (opts.type === "data") {
            return new DataChunk(opts);
        } else if (opts.type === "error") {
            return new ErrorChunk(opts);
        } else if (opts.type === "metadata") {
            return new MetadataChunk(opts);
        }

        throw new Error("not reached");
    }
}

/**
 * Represents a blob of any data. Data is always an Object
 */
export class DataChunk extends Chunk {
    data: ChunkData = {};
    type: ChunkType = "data";

    /**
     * Creates a new Chunk of data
     *
     * @param opt - The configuration options of this chunk
     * @returns Chunk
     */
    constructor(opt: DataChunkOptions) {
        super("data", opt);

        this.data = opt.data ?? this.data;

        if (opt.data instanceof DataChunk) {
            return opt.data;
        }
    }

    /**
     * Creates an identical but different version of this Chunk
     */
    clone(): Chunk {
        return Chunk.create({
            type: "data",
            data: structuredClone(this.data),
        });
    }
}

/**
 * Represents data that has become an error
 */
export class ErrorChunk extends Chunk {
    data: ChunkData;
    error: Error;
    type: ChunkType = "error";

    /**
     * Creates a new Chunk of data
     *
     * @param opt - The configuration options of this chunk
     * @returns Chunk
     */
    constructor(opt: ErrorChunkOptions) {
        super("error", opt);

        this.error = opt.error;
        this.data = opt.data;
    }
}

/**
 * Represents metadata about a stream
 */
export class MetadataChunk extends Chunk {
    /**
     * Creates a new MetadataChunk
     *
     * @param opt - The metadata chunk options
     */
    constructor(opt: MetadataChunkOptions) {
        super("metadata", opt);

        throw new Error("metadata not implemented");
    }
}

export type ChunkCollectionForEachCb = (chunk: Chunk, chNum: number) => void;

/**
 * A group of data, sorted by which OutputChannel it will be sent on. Using this is more effecient
 * than sending data serially one channel at a time.
 */
export class ChunkCollection {
    chunks: Map<number, Chunk> = new Map();

    /**
     * Adds data to the collection
     *
     * @param chNum - The channel number to add data to. Throws an Error if data has already been added to that channel.
     * @param chunk -  The data to add.
     */
    add(chNum: number, chunk: Chunk): void {
        if (this.chunks.has(chNum)) {
            throw new Error(`ChunkCollection already has message on channel: ${chNum}`);
        }

        this.chunks.set(chNum, chunk);
    }

    /**
     * Gets the data associated with a channel
     *
     * @param chNum - The channel number to get data for
     * @returns The data for the specified channel or undefined if none is set.
     */
    get(chNum: number): Chunk | undefined {
        return this.chunks.get(chNum);
    }

    /**
     * Iterates the data Chunks that have been set in this collection
     *
     * @param cb - Called for each Chunk
     */
    forEach(cb: ChunkCollectionForEachCb): void {
        this.chunks.forEach(cb);
    }
}
