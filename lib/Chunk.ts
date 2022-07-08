// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ChunkData = Record<string, any>;
export type ChunkType = "data" | "error" | "metadata";

export interface DataChunkOptions {
    data: ChunkData;
}

export interface ErrorChunkOptions {
    data: ChunkData;
    error: Error;
}

export interface Chunk extends DataChunk, ErrorChunk, MetadataChunk {}
export class Chunk {
    isData(): this is DataChunk {
        return this.type === "data";
    }

    isError(): this is ErrorChunk {
        return this.type === "error";
    }

    isMetadata(): this is MetadataChunk {
        return this.type === "metadata";
    }
}

/**
 * Represents a blob of any data. Data is always an Object
 */
export class DataChunk {
    data: ChunkData = {};
    type: ChunkType = "data";

    /**
     * Creates a new Chunk of data
     *
     * @param opt The configuration options of this chunk
     * @returns Chunk
     */
    constructor(opt: DataChunkOptions) {
        this.data = opt.data ?? this.data;

        if (opt.data instanceof DataChunk) {
            return opt.data;
        }
    }

    /**
     * Creates an identical but different version of this Chunk
     */
    clone() {
        return new DataChunk({
            data: structuredClone(this.data),
        });
    }
}

/**
 * Represents data that has become an error
 */
export class ErrorChunk {
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
        this.error = opt.error;
        this.data = opt.data;
    }
}

/**
 * Represents metadata about a stream
 */
export class MetadataChunk {
    type: ChunkType = "metadata";
}
