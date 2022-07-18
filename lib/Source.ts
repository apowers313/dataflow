import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {Component} from "./Component";
import {walkStream} from "./utils";

type FinishedFn = () => Promise<void>

export interface SourceMethods extends ReadMethods {
    finished: FinishedFn;
}

/**
 * Generates data for a pipeline
 */
export class Source extends Readable(Component) {
    declare methods: SourceMethods;

    /**
     * Creates a source
     *
     * @param opts Options for the new source
     */
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    constructor(opts: ReadableOpts) {
        super(opts);

        this!.methods.finished = async(): Promise<void> => {
            this.controller!.close();
        };
    }

    async complete(): Promise<void> {
        const initPromises: Array<Promise<void>> = [];
        walkStream(this, (c) => {
            const p = c.init();
            initPromises.push(p);
        });

        await Promise.all(initPromises);
    }
}
