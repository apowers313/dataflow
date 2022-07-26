import {Chunk, ChunkCollection, MetadataChunk} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import {DataflowEnd} from "./Metadata";
import {DeferredPromise} from "./utils";
import {ReadableStream} from "node:stream/web";
import type {WritableType} from "./Writable";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = abstract new (... args: any[]) => T;

export type PullFn = (methods: ReadMethods) => Promise<void>

export interface ReadableOpts extends ComponentOpts {
    readStart?: (controller: WritableStreamDefaultController) => Promise<void>;
    readClose?: () => Promise<void>;
    readCancel?: () => Promise<void>;
    numChannels?: number;
    pull: PullFn;
}

type SendFn = (chNum: number, data: Chunk) => Promise<void>
type SendMultiFn = (cc: ChunkCollection) => Promise<void>

export interface ReadMethods {
    send: SendFn;
    sendMulti: SendMultiFn;
}

/**
 * Applies the Reader mixin to a base class
 *
 * @param Base - The base class the mixin will be applied to
 * @returns A Reader class that extends specified base class
 */
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export function Readable<TBase extends Constructor<Component>>(Base: TBase) {
    /**
     * Creates a stream that can be read from
     */
    abstract class Reader extends Base {
        readonly isReadable = true;
        readonly numChannels: number = 1;
        readonly readMethods: ReadMethods = {
            send: async(chNum, data) => {
                const cc = new ChunkCollection();
                cc.add(chNum, data);
                this.readableController.enqueue(cc);
            },
            sendMulti: async(cc: ChunkCollection) => {
                this.readableController.enqueue(cc);
            },
        };

        done = false;
        channels: Array<OutputChannel>;
        readableController!: ReadableStreamDefaultController;

        #pendingReads: Map<Output, DeferredPromise<Chunk>> = new Map();
        #pull: PullFn;
        #readableStream: ReadableStream<ChunkCollection>;
        #reader: ReadableStreamDefaultReader<ChunkCollection>;

        /**
         * Creates a new Reader
         *
         * @param args - Options for the new reader
         */
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: ReadableOpts = args[0] ?? {};

            this.#pull = cfg.pull;

            this.numChannels = cfg.numChannels ?? this.numChannels;
            this.channels = new Array(this.numChannels)
                .fill(null)
                .map((_o: unknown, idx: number) => new OutputChannel({chNum: idx, parent: this}));

            this.#readableStream = new ReadableStream({
                start: async(controller): Promise<void> => {
                    this.readableController = controller;
                    if (typeof cfg.readStart === "function") {
                        await cfg.readStart(controller);
                    }
                },
                pull: async(): Promise<void> => {
                    return this.#pull(this.readMethods);
                },
                cancel: cfg.readCancel,
            });
            this.#reader = this.#readableStream.getReader();
        }

        /**
         * Initializes the Readable
         */
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        async init(): Promise<void> {
            await super.init();
            console.log(`Readable init (${this.name})`);
        }

        /** All of the Writable destinations associated with this Readable  */
        get dests(): Array<WritableType> {
            let ret: Array<WritableType> = [];
            this.channels.forEach((c) => {
                ret = ret.concat(c.dests);
            });

            return ret;
        }

        /** Number of destinations that have been connected to this Readable via pipe() */
        get numDests(): number {
            return this.dests.length;
        }

        /**
         * Reads data for a specified output. Promise resolves when data has been received on the OutputChannel
         * associated with the output.
         *
         * @param dest - The Output to read data for.
         * @returns The data that has been received
         */
        async readFor(dest: Output): Promise<Chunk> {
            if (this.done) {
                const md = Chunk.create({type: "metadata"}) as MetadataChunk;
                md.metadata.add(new DataflowEnd());
                return md;
            }

            console.log("READ FOR", dest.channel.chNum, "by", this.name);
            const existing = this.#pendingReads.get(dest);
            if (existing) {
                return existing.promise;
            }

            const dp = new DeferredPromise<Chunk>();
            this.#pendingReads.set(dest, dp);

            if ((this.#pendingReads.size) >= this.numDests) {
                await this.#doRead();
            }

            return dp.promise;
        }

        /**
         * Internal helper function for reading data and sending it to all the correct Outputs
         */
        async #doRead(): Promise<void> {
            console.log("doRead");
            const readData = await this.#reader.read();
            if (readData.done) {
                // wrap things up
                this.done = true;
                const md = Chunk.create({type: "metadata"}) as MetadataChunk;
                md.metadata.add(new DataflowEnd());
                this.#pendingReads.forEach((deferredPromise) => {
                    deferredPromise.resolve(md);
                });
                return;
            }

            const cc = readData.value;
            if (!cc) {
                throw new Error("data read returned undefined");
            }

            console.log("cc", cc);
            cc.forEach((data, chNum) => {
                [... this.#pendingReads]
                    // find the matching reads for the channel
                    .filter((pr) => pr[0].channel.chNum === chNum)
                    // send data for each of the pending readers and remove the reader from the pending list
                    .forEach((tuple) => {
                        const [output, dp] = tuple;
                        console.log("sending on:", chNum);
                        this.#pendingReads.delete(output);
                        dp.resolve(data);
                    });
            });
        }
    }
    return Reader;
}

export interface OutputChannelOpts {
    chNum: number;
    parent: ReadableType;
}

/**
 * Each Readable (Source or Through) may have multiple output channels. Output channels send distinct data and
 * may be used for de-multiplexing one data stream into multiple data streams.
 */
export class OutputChannel {
    /** The channel number of this channel in relation to all the channels owned by the Readable */
    chNum: number;
    /** The parent source that 'owns' this channel */
    source: ReadableType;
    /** All the outputs that will generate copies of the data for this channel */
    outputs: Array<Output> = [];

    /**
     * Creates a new output stream
     *
     * @param opt - options for the new output stream
     */
    constructor(opt: OutputChannelOpts) {
        this.chNum = opt.chNum;
        this.source = opt.parent;
    }

    /**
     * Connects a source to a destination, so that all the data generated by the source will be received by the destination.
     *
     * @param dst - The Writeable (Sink or Through) to connect the data to. May be a single output or an array of outputs.
     */
    pipe(dst: WritableType | Array<WritableType>): void {
        if (!Array.isArray(dst)) {
            dst = [dst];
        }

        dst.forEach((d) => {
            const o = new Output({channel: this, dest: d});
            this.outputs.push(o);
            d.connect(o);
        });
    }

    /** All the destinations associated with this OutputChannel */
    get dests(): Array<WritableType> {
        let ret: Array<WritableType> = [];
        this.outputs.forEach((o) => {
            ret = ret.concat(o.dest);
        });

        return ret;
    }

    // async init(): Promise<void> {}
}

export interface OutputOpts {
    channel: OutputChannel;
    dest: WritableType;
}

/**
 * An output of a Readable. A single {@link OutputChannel} may have multiple Outputs,
 * each that sends its own copy of the exact same data.
 */
export class Output {
    channel: OutputChannel;
    source: ReadableType;
    dest: WritableType;

    /**
     * Creates a new Output.
     *
     * @param opts - Options for the new Output.
     */
    constructor(opts: OutputOpts) {
        this.channel = opts.channel;
        this.source = this.channel.source;
        this.dest = opts.dest;
    }

    /**
     * Reads data from the source. Promise will only resolve once data has
     * been written to this specific channel.
     *
     * @returns The data that has been read
     */
    async read(): Promise<Chunk> {
        const data = await this.source.readFor(this);
        if (data.isData()) {
            return data.clone();
        }

        // TODO: should we clone errors too?
        return data;
    }
}

export type ReadableType = InstanceType<ReturnType<typeof Readable>>
// export class ReadableType extends Readable(Component) {}
