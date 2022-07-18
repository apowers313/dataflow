import {Chunk, ChunkData} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import type {ReadableType} from "./Readable";
import {WritableStream} from "node:stream/web";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = abstract new (... args: any[]) => T;

type PushFn = (data: ChunkData, methods: WriteMethods) => Promise<void>

export interface WritableOpts extends ComponentOpts {
    start?: (controller: WritableStreamDefaultController) => void;
    close?: () => void;
    abort?: () => void;
    push: PushFn;
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface WriteMethods {}

/**
 * Applies the Writer mixin to a base class
 *
 * @param Base The base class the mixin will be applied to
 * @returns Writer
 */
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export function Writable<TBase extends Constructor<Component>>(Base: TBase) {
    abstract class Writer extends Base {
        readonly isWritable = true;
        numInputs = 0;
        push: PushFn;
        writableStream: WritableStream;
        controller?: WritableStreamDefaultController;
        methods: WriteMethods = {};
        inputs: Array<InputChannel> = [];

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: WritableOpts = args[0] ?? {};

            this.push = cfg.push;
            this.writableStream = new WritableStream({
                start: (controller): void => {
                    this.controller = controller;
                    if (typeof cfg.start === "function") {
                        cfg.start(controller);
                    }
                },
                write: async(chunk): Promise<void> => {
                    if (!(chunk instanceof Chunk)) {
                        throw new TypeError("DataflowSink: expected write data to be instance of DataflowChunk");
                    }

                    // console.log(`sink '${this.name}':`, chunk.data);
                    if (chunk.isData()) {
                        await this.push(chunk.data, this.methods);
                    }
                },
                close: cfg.close,
                abort: cfg.abort,
            });
        }

        async init(): Promise<void> {
            const promises = this.inputs.map((i) => {
                return i.init();
            });
            promises.push(this.#run());

            await Promise.all(promises);
        }

        // eslint-disable-next-line jsdoc/require-jsdoc
        async #run(): Promise<void> {
            console.log("writable stream running");
        }

        connect(src: ReadableType) {
            const ic = new InputChannel({});
            this.inputs.push(ic);
            return ic.getWriter();
        }

        get srcs(): Array<ReadableType> {
            return this.inputs.map((i) => i.source);
        }
    }

    return Writer;
}

interface InputChannelOpts {
    push: PushFn;
    source: ReadableType;
}

export class InputChannel {
    #writableStream: WritableStream;
    push: PushFn;
    source: ReadableType;
    controller?: WritableStreamDefaultController;
    methods: WriteMethods = {};

    constructor(opts: InputChannelOpts) {
        this.writableStream = new WritableStream({
            start: (controller): void => {
                this.controller = controller;
                if (typeof cfg.start === "function") {
                    cfg.start(controller);
                }
            },
            write: async(chunk): Promise<void> => {
                if (!(chunk instanceof Chunk)) {
                    throw new TypeError("DataflowSink: expected write data to be instance of DataflowChunk");
                }

                // console.log(`sink '${this.name}':`, chunk.data);
                if (chunk.isData()) {
                    await this.push(chunk.data, this.methods);
                }
            },
        });
    }

    async init(): Promise<void> {
        return pipePromise;
    }

    getWriter() {
        return this.#writableStream.getWriter();
    }

    getReader() {

    }
}

// export type WritableType = InstanceType<ReturnType<typeof Writable>>
export class WritableType extends Writable(Component) {}
