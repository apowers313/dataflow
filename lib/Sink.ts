import {Writable, WritableOpts} from "./Writable";
import {Component} from "./Component";

/**
 * The end of a pipeline
 */
export class Sink extends Writable(Component) {
    /**
     * Creates a new Sink
     *
     * @param opts - Options for the Sink
     */
    constructor(opts: WritableOpts) {
        super(opts);

        console.log("sink!");
    }
}
