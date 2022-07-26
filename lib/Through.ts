import {Component, ComponentOpts} from "./Component";
import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {Writable, WritableOpts, WriteMethods} from "./Writable";
import {Chunk} from "./Chunk";
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
    #writePromise = new DeferredPromise<Chunk>();
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
                console.log("through top-level push");
                await this.#throughPush.call(this, data, methods);
            },
            pull: async(methods): Promise<void> => {
                console.log("through top-level pull");
                await this.#throughPull.call(this, methods);
            },
        };
        inputOpts.writeClose = async(): Promise<void> => {
            console.log("closing readable controller");
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
    async #throughPush(chunk: Chunk, _methods: WriteMethods): Promise<void> {
        console.log(">>> THROUGH push got", chunk);
        this.#writePromise.resolve(chunk);
        await this.#readDone.promise;
        this.#writePromise = new DeferredPromise<Chunk>();
        this.#readDone = new DeferredPromise<null>();
    }

    /**
     * Sends data consumer(s) of the ReadableStream
     *
     * @param methods - Functions for manipulating the stream and data
     */
    async #throughPull(methods: ReadMethods): Promise<void> {
        console.log("--- THROUGH PULL");
        const chunk = await this.#writePromise.promise;
        console.log(">>> THROUGH pull got:", chunk);
        if (chunk.isData()) {
            await this.#through(chunk, methods);
        }

        this.#readDone.resolve(null);
    }
}
