// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ChunkData = Record<string, any>;
export type ChunkType = "data" | "error" | "metadata";

export interface ChunkOptions {
    data?: ChunkData;
    error?: Error;
    type?: ChunkType;
}

/**
 * Represents a blob of any data. Data is always an Object
 */
export class Chunk {
    data: ChunkData | undefined;
    error: Error | undefined;
    type: ChunkType;

    /**
     * Creates a new Chunk of data
     *
     * @param opt The configuration options of this chunk
     * @returns Chunk
     */
    constructor(opt: ChunkOptions = {}) {
        this.type = opt.type ?? "data";
        this.data = opt.data ?? {};

        if (opt.data instanceof Chunk) {
            return opt.data;
        }

        if (opt.error instanceof Error) {
            this.error = opt.error;
            this.type = "error";
            return this;
        }
    }

    /**
     * Creates an identical but different version of this Chunk
     */
    clone() {
        return new Chunk({
            data: structuredClone(this.data),
            type: this.type,
            error: this.error,
        });
    }
}
