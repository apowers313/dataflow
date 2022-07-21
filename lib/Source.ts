import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {Component} from "./Component";
import {walkStream} from "./utils";

type FinishedFn = () => Promise<void>
export type SourcePullFn = (methods: SourceMethods) => Promise<void>

export interface SourceMethods extends ReadMethods {
    finished: FinishedFn;
}

export interface SourceOpts extends Omit<ReadableOpts, "pull"> {
    pull: SourcePullFn;
}

/**
 * Generates data for a pipeline
 */
export class Source extends Readable(Component) {
    declare methods: SourceMethods;
    declare pull: SourcePullFn;

    /**
     * Creates a source
     *
     * @param opts - Options for the new source
     */
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    constructor(opts: SourceOpts) {
        super(opts);

        console.log("this.methods", this.methods);
        this.methods.finished = async(): Promise<void> => {
            this.controller!.close();
        };
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
