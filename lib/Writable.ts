import {Component, ComponentOpts} from "./Component";
import type {Output, ReadableType} from "./Readable";
import {Chunk} from "./Chunk";
import {DataflowEnd} from "./Metadata";
import {WritableStream} from "node:stream/web";
import {promiseState} from "./utils";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = abstract new (... args: any[]) => T;

type PushFn = (chunk: Chunk, methods: WriteMethods) => Promise<void>

export interface WritableOpts extends ComponentOpts {
    writeStart?: (controller: WritableStreamDefaultController) => void;
    writeClose?: () => Promise<void>;
    writeAbort?: (reason?: Error) => Promise<void>;
    push: PushFn;
}

export type InputMuxModes = "fifo" | "zipper" | "batch";

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
        readonly inputMuxMode: InputMuxModes = "fifo";
        readonly writeMethods: WriteMethods = {};

        inputs: Array<Output> = [];
        writableController?: WritableStreamDefaultController;

        #push: PushFn;
        #writableStream: WritableStream<Chunk>;
        #writer: WritableStreamDefaultWriter<Chunk>;

        /**
         * Creates a Writable type
         *
         * @param args - Options for the new writable type
         */
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const cfg: WritableOpts = args[0] ?? {};

            this.#push = cfg.push;
            this.#writableStream = new WritableStream({
                start: async(controller): Promise<void> => {
                    this.writableController = controller;
                    if (typeof cfg.writeStart === "function") {
                        await cfg.writeStart(controller);
                    }
                },
                write: async(chunk): Promise<void> => {
                    if (!(chunk instanceof Chunk)) {
                        throw new TypeError("DataflowSink: expected write data to be instance of DataflowChunk");
                    }

                    if (chunk.isData()) {
                        await this.#push(chunk, this.writeMethods);
                    }
                },
                close: cfg.writeClose,
                abort: cfg.writeAbort,
            });
            this.#writer = this.#writableStream.getWriter();
        }

        /** All the direct sources that will send data to this Writable */
        get srcs(): Array<ReadableType> {
            return this.inputs.map((i) => i.channel.source);
        }

        /**
         * Initializes the Writable
         */
        async init(): Promise<void> {
            await super.init();
            console.log(`Writable init (${this.name})`);
            return this.#run();
        }

        // eslint-disable-next-line jsdoc/require-jsdoc
        async #run(): Promise<void> {
            console.log("#RUNNING", this.name);
            const activeReaders = [... this.inputs];
            const readerPromises = activeReaders.map((r) => r.read());
            const mode = this.inputMuxMode;
            const writer = this.#writer;

            const processStreams = async(): Promise<void> => {
                const results = await getResults();
                console.log("read results:", results);

                // forward-loop sending results to maintain order of messages
                const batch: Array<Chunk> = [];
                for (let i = 0; i < results.length; i++) {
                    const chunk = results[i];
                    if (chunk === null || !chunk.isData()) {
                        continue;
                    }

                    if (mode === "batch") {
                        batch.push(chunk);
                    } else {
                        console.log("writing chunk", chunk);
                        console.log("writer wants", writer.desiredSize);
                        await writer.write(chunk);
                    }

                    readerPromises[i] = activeReaders[i].read();
                }

                if (mode === "batch" && batch.length) {
                    const batchData: Record<number, Chunk> = {};
                    batch.forEach((chunk, idx) => {
                        batchData[idx] = chunk;
                    });
                    await writer.write(Chunk.create({type: "data", data: batchData}));
                }

                // backward-loop removing dead streams; forward-removing would make indexes invalid
                for (let i = results.length - 1; i >= 0; i--) {
                    const chunk = results[i];
                    if (chunk !== null && chunk.isMetadata() && chunk.metadata.has(DataflowEnd)) {
                        console.log("removing reader", i);
                        removeReader(i);
                    }
                }

                console.log("activeReaders.length", activeReaders.length);
                if (activeReaders.length === 0) {
                    // no more readers, all done!
                    await this.#writer.close();
                    return;
                }

                // eslint-disable-next-line consistent-return
                return waitForReaders().then(processStreams);
            };

            const waitForReaders = async(): Promise<void> => {
                switch (mode) {
                case "fifo":
                    await Promise.race(readerPromises);
                    return;
                case "batch":
                case "zipper":
                    await Promise.all(readerPromises);
                    return;
                default:
                    throw new TypeError(`unknown DataflowMultiInput mode: ${mode}`);
                }
            };

            const getResults = async(): Promise<Array<Chunk|null>> => {
                const promiseStates = await Promise.all(readerPromises.map((r) => promiseState(r)));
                const results: Array<Chunk|null> = [];

                for (let i = 0; i < promiseStates.length; i++) {
                    switch (promiseStates[i]) {
                    case "pending":
                        results.push(null);
                        break;
                    case "fulfilled":
                        results.push(await readerPromises[i]);
                        break;
                    case "rejected":
                        results.push(null);
                        // TODO: do we want to do something different here?
                        // results.push({done: "error"});
                        break;
                    default:
                        throw new Error("unknown promise state");
                    }
                }

                return results;
            };

            const removeReader = function(n: number): void {
                readerPromises.splice(n, 1);
                activeReaders.splice(n, 1);
            };

            return waitForReaders().then(processStreams);
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
