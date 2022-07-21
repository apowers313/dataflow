import {Chunk, ChunkData} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import type {Output, ReadableType} from "./Readable";
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
 * @param Base - The base class the mixin will be applied to
 * @returns Writer
 */
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export function Writable<TBase extends Constructor<Component>>(Base: TBase) {
    abstract class Writer extends Base {
        readonly isWritable = true;
        push: PushFn;
        writableStream: WritableStream;
        methods: WriteMethods = {};
        inputs: Array<Output> = [];

        #controller?: WritableStreamDefaultController;

        /**
         * Creates a Writable type
         *
         * @param args - Options for the new writable type
         */
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: WritableOpts = args[0] ?? {};

            this.push = cfg.push;
            this.writableStream = new WritableStream({
                start: (controller): void => {
                    this.#controller = controller;
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

        /** All the direct sources that will send data to this Writable */
        get srcs(): Array<ReadableType> {
            return this.inputs.map((i) => i.channel.source);
        }

        /**
         * Initializes the Writable
         */
        async init(): Promise<void> {
            console.log("Writable init");
            return this.#run();
        }

        // eslint-disable-next-line jsdoc/require-jsdoc
        async #run(): Promise<void> {
            console.log("writable stream running");
            throw new Error("not implemented");
        }

        /**
         * Connects this Writable to a Readable so that the Writable receives data from a Readable
         *
         * @param input - The Readable to receive data from
         */
        connect(input: Output): void {
            this.inputs.push(input);
        }
    }

    return Writer;
}

/** The type used to identify Writables */
export class WritableType extends Writable(Component) {}
