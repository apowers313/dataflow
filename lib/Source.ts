import {Component, ComponentOpts} from "./Component";
import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {walkStream} from "./utils";

type FinishedFn = () => Promise<void>
export type SourcePullFn = (methods: SourceMethods) => Promise<void>

export interface SourceMethods extends ReadMethods {
    finished: FinishedFn;
}

export interface SourceOpts extends Omit<ReadableOpts, "pull"> {
    pull: SourcePullFn;
}

type SourceSuperOpts = ReadableOpts & ComponentOpts

/**
 * Generates data for a pipeline
 */
export class Source extends Readable(Component) {
    #sourcePull: SourcePullFn;

    /**
     * Creates a source
     *
     * @param opts - Options for the new source
     */
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    constructor(opts: SourceOpts) {
        const inputOpts: SourceSuperOpts = {
            ... opts,
            pull: async(methods): Promise<void> => {
                await this.#sourcePull({
                    ... methods,
                    finished: async(): Promise<void> => {
                        this.readableController!.close();
                    },
                });
            },
        };
        super(inputOpts);
        this.#sourcePull = opts.pull;

        console.log("this.methods", this.readMethods);
    }

    /**
     * Waits for a dataflow to finish sending all data
     */
    async complete(): Promise<void> {
        const initPromises: Array<Promise<void>> = [];
        walkStream(this, (c) => {
            const p = c.init();
            initPromises.push(p);
        });

        await Promise.all(initPromises);
    }
}
