import {Chunk, ChunkCollection} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {Writable, WritableOpts, WriteMethods} from "./Writable";
import {DeferredPromise} from "./utils";

export type ThroughMethods = ReadMethods & WriteMethods;
export type ThroughFn = (chunk: Chunk, methods: ThroughMethods) => Promise<void>
export interface ThroughOpts extends Omit<ReadableOpts, "pull">, Omit<WritableOpts, "push"> {
    through: ThroughFn;
}

type ThroughSuperOpts = ReadableOpts & WritableOpts & ComponentOpts;

/**
 * A component that reads data from a input, acts on it, and then passes it to an output
 */
export class Through extends Writable(Readable(Component)) {
    #through: ThroughFn;
    #writePromise = new DeferredPromise<Chunk|ChunkCollection>();
    #readDone = new DeferredPromise<null>();

    /**
     * Creates a new through stream
     *
     * @param opts - Options for the new through stream
     */
    constructor(opts: ThroughOpts) {
        const inputOpts: ThroughSuperOpts = {
            ... opts,
            push: async(data, methods): Promise<void> => {
                await this.#throughPush(data, methods);
            },
            pull: async(methods): Promise<void> => {
                await this.#throughPull(methods);
            },
        };
        inputOpts.writeClose = async(): Promise<void> => {
            this.readableController.close();
            if (opts.writeClose) {
                await opts.writeClose();
            }
        };
        super(inputOpts);

        this.#through = opts.through;
    }

    /**
     * Receives data from a WritableStream
     *
     * @param chunk - Data received as input
     * @param _methods - Not used
     */
    async #throughPush(chunk: Chunk|ChunkCollection, _methods: WriteMethods): Promise<void> {
        this.#writePromise.resolve(chunk);
        await this.#readDone.promise;
        this.#writePromise = new DeferredPromise<Chunk|ChunkCollection>();
        this.#readDone = new DeferredPromise<null>();
    }

    /**
     * Sends data consumer(s) of the ReadableStream
     *
     * @param methods - Functions for manipulating the stream and data
     */
    async #throughPull(methods: ReadMethods): Promise<void> {
        const chunk = await this.#writePromise.promise;

        if (chunk instanceof ChunkCollection) {
            throw new Error("Through received ChunkCollection. Why?");
        }

        if (chunk.isData()) {
            await this.#through(chunk, methods);
        }

        this.#readDone.resolve(null);
    }
}
