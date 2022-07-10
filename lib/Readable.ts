import {Chunk, ChunkData} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import {ReadableStream} from "node:stream/web";
import type {WritableType} from "./Writable";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = new (... args: any[]) => T;

export type PullFn = (methods: ReadMethods) => Promise<void>

export interface ReadableOpts extends ComponentOpts {
    start?: (controller: WritableStreamDefaultController) => void;
    close?: () => void;
    cancel?: () => void;
    numOutputs?: number;
    pull: PullFn;
}

type SendToFn = (chNum: number, data: Chunk | ChunkData) => Promise<void>
type SendSingleOuputFn = (data: Chunk | ChunkData) => Promise<void>
type SendFn = SendToFn | SendSingleOuputFn
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
export function Readable<TBase extends Constructor<Component>>(Base: TBase) {
    /**
     * Creates a stream that can be read from
     */
    return class Reader extends Base {
        readonly isReadable = true;
        numOutputs = 1;
        outputs: Array<OutputChannel>;
        controller?: ReadableStreamDefaultController;
        readableStream: ReadableStream;
        methods: ReadMethods = {
            send: async(data: ChunkData) => {
                data = Chunk.create({type: "data", data});
                this.controller!.enqueue(data);
            },
            sendReady: async() => {
                throw new Error("sendReady not implemented");
            },
        };

        #pull: PullFn;

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: ReadableOpts = args[0] ?? {};

            this.#pull = cfg.pull;

            this.numOutputs = cfg.numOutputs ?? this.numOutputs;
            this.outputs = new Array(this.numOutputs)
                .fill(null)
                .map((_o: unknown, idx: number) => new OutputChannel({chNum: idx, parent: this}));

            const streamCfg = {
                start: (controller: ReadableStreamDefaultController) => {
                    this.controller = controller;
                    if (typeof cfg.start === "function") {
                        cfg.start(controller);
                    }
                },
                pull: async() => {
                    return this.#pull(this.methods);
                },
                cancel: cfg.cancel,
            };

            this.readableStream = new ReadableStream(streamCfg);
        }

        async init() {
            await super.init();
        }

        get dests(): Array<WritableType> {
            let ret: Array<WritableType> = [];
            this.outputs.forEach((c) => {
                ret = ret.concat(c.dests);
            });

            return ret;
        }
    };
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

    /**
     * Creates a new output stream
     *
     * @param opt options for the new output stream
     */
    constructor(opt: OutputChannelOpts) {
        this.chNum = opt.chNum;
        this.parent = opt.parent;
    }

    pipe(dst: WritableType | Array<WritableType>) {
        if (!Array.isArray(dst)) {
            dst = [dst];
        }

        dst.forEach((d) => {
            d.addSource(this.parent);
        });

        this.dests = this.dests.concat(dst);
    }
}

// TODO: InstanceType<Type>
export class ReadableType extends Readable(Component) {}
