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

export abstract class Chunk {
    type: ChunkType;
    opt: ChunkOptions;

    constructor(type: ChunkType, opt: ChunkOptions) {
        this.type = type;

        // TODO: kill this
        this.opt = opt;
    }

    isData(): this is DataChunk {
        return this.type === "data";
    }

    isError(): this is ErrorChunk {
        return this.type === "error";
    }

    isMetadata(): this is MetadataChunk {
        return this.type === "metadata";
    }

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
     * @param opt The configuration options of this chunk
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
    clone() {
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
     * @param opt The configuration options of this chunk
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
    constructor(opt: MetadataChunkOptions) {
        super("metadata", opt);

        throw new Error("metadata not implemented");
    }
}
