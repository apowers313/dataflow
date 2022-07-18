import {Chunk, ChunkData} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import {ReadableStream} from "node:stream/web";
import type {WritableType} from "./Writable";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = abstract new (... args: any[]) => T;

export type PullFn = (methods: ReadMethods) => Promise<void>

export interface ReadableOpts extends ComponentOpts {
    start?: (controller: WritableStreamDefaultController) => void;
    close?: () => void;
    cancel?: () => void;
    numOutputs?: number;
    pull: PullFn;
}

type SendFn = (chNum: number, data: Chunk | ChunkData) => Promise<void>
type SendReadyFn = () => Promise<void>

export interface ReadMethods {
    send: SendFn;
    sendReady: SendReadyFn;
}

/**
 * Applies the Reader mixin to a base class
 *
 * @param Base The base class the mixin will be applied to
 * @returns Reader
 */
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export function Readable<TBase extends Constructor<Component>>(Base: TBase) {
    /**
     * Creates a stream that can be read from
     */
    abstract class Reader extends Base {
        readonly isReadable = true;
        numOutputs = 1;
        outputs: Array<OutputChannel>;
        controller?: ReadableStreamDefaultController;
        methods: ReadMethods = {
            send: async(_chNum, data) => {
                console.log("not using chNum, please fix", _chNum);
                data = Chunk.create({type: "data", data});
                this.controller!.enqueue(data);
            },
            sendReady: async() => {
                throw new Error("sendReady not implemented");
            },
        };

        #pull: PullFn;
        #reader: ReadableStreamDefaultReader;
        #readableStream: ReadableStream;

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: ReadableOpts = args[0] ?? {};

            this.#pull = cfg.pull;

            this.numOutputs = cfg.numOutputs ?? this.numOutputs;
            this.outputs = new Array(this.numOutputs)
                .fill(null)
                .map((_o: unknown, idx: number) => new OutputChannel({chNum: idx, parent: this}));

            this.#readableStream = new ReadableStream({
                start: (controller): void => {
                    this.controller = controller;
                    if (typeof cfg.start === "function") {
                        cfg.start(controller);
                    }
                },
                pull: async(): Promise<void> => {
                    return this.#pull(this.methods);
                },
                cancel: cfg.cancel,
            });
            this.#reader = this.#readableStream.getReader();
        }

        async init(): Promise<void> {
            return this.#run();
        }

        // eslint-disable-next-line jsdoc/require-jsdoc
        async #run(): Promise<void> {
            const doWrite = async(): Promise<void> => {
                const streamData = await this.#reader.read();
                console.log("readable stream got data");

                if (streamData.done) {
                    console.log("readable stream done");
                    await (this.sendReady());
                    return;
                }

                console.log("readable stream ignoring data");
            };

            return this.sendReady().then(doWrite);
        }

        async sendReady(): Promise<void> {
            const outputsReady = this.outputs.map((o) => {
                return o.outputReady();
            });

            // TODO: this might be more effecient as a Promise.race()
            // and then the sender has to await the specific output before
            // sending another chunk
            await Promise.all(outputsReady);
        }

        get dests(): Array<WritableType> {
            let ret: Array<WritableType> = [];
            this.outputs.forEach((c) => {
                ret = ret.concat(c.dests);
            });

            return ret;
        }
    }
    return Reader;
}

export interface OutputChannelOpts {
    chNum: number;
    parent: ReadableType;
}

/**
 * An output stream
 */
export class OutputChannel {
    chNum: number;
    parent: ReadableType;
    dests: Array<WritableType> = [];

    #writers: Array<WritableStreamDefaultWriter> = [];

    /**
     * Creates a new output stream
     *
     * @param opt options for the new output stream
     */
    constructor(opt: OutputChannelOpts) {
        this.chNum = opt.chNum;
        this.parent = opt.parent;
    }

    pipe(dst: WritableType | Array<WritableType>): void {
        if (!Array.isArray(dst)) {
            dst = [dst];
        }

        const writers = dst.map((d) => {
            return d.connect(this.parent);
        });

        this.dests = this.dests.concat(dst);
        this.#writers = this.#writers.concat(writers);
    }

    init() {}

    async outputReady(): Promise<void> {
        return Promise.all(this.#writers);
    }
}

class OutputStream {
    dest: WritableType;
    #readableStream: ReadableStream;

    constructor(dest: WritableType) {
        this.#readableStream = new ReadableStream({});
        this.dest = dest;
        // destWriter
    }

    async init() {
        return this.#readableStream.pipeTo(this.dest.getStream());
    }
}

export type ReadableType = InstanceType<ReturnType<typeof Readable>>
// export class ReadableType extends Readable(Component) {}
