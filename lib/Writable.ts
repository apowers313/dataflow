import {Chunk, ChunkData} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import type {ReadableType} from "./Readable";
import {WritableStream} from "node:stream/web";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = new (... args: any[]) => T;

type PushFn = (data: ChunkData, methods: WriteMethods) => Promise<void>

export interface WritableOpts extends ComponentOpts {
    numInputs?: number;
    start?: (controller: WritableStreamDefaultController) => void;
    close?: () => void;
    abort?: () => void;
    push: PushFn;
}

export interface WriteMethods {
    send?: () => Promise<void>
}

/**
 * Applies the Writer mixin to a base class
 *
 * @param Base The base class the mixin will be applied to
 * @returns Writer
 */
export function Writable<TBase extends Constructor<Component>>(Base: TBase) {
    return class Writer extends Base {
        readonly isWritable = true;
        numInputs = 0;
        push: PushFn;
        writableStream: WritableStream;
        controller?: WritableStreamDefaultController;
        methods: WriteMethods = {};
        srcs: Array<ReadableType> = [];

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: WritableOpts = args[0] ?? {};

            this.push = cfg.push;
            this.numInputs = cfg.numInputs ?? this.numInputs;
            this.writableStream = new WritableStream({
                start: (controller) => {
                    this.controller = controller;
                    if (typeof cfg.start === "function") {
                        cfg.start(controller);
                    }
                },
                write: async(chunk) => {
                    if (!(chunk instanceof Chunk)) {
                        throw new TypeError("DataflowSink: expected write data to be instance of DataflowChunk");
                    }

                    if (chunk.type !== "data") {
                        return;
                    }

                    // console.log(`sink '${this.name}':`, chunk.data);
                    if (chunk.type === "data") {
                        await this.push(chunk.data, this.methods);
                    }
                },
                close: cfg.close,
                abort: cfg.abort,
            });
        }

        addSource(src: ReadableType) {
            this.srcs.push(src);
        }
    };
}

export class WritableType extends Writable(Component) {}
