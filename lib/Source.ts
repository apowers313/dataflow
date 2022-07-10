import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {Component} from "./Component";

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
    constructor(opts: ReadableOpts) {
        super(opts);

        this!.methods.finished = async() => {
            this.controller!.close();
        };
    }
}
