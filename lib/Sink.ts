import {Writable, WritableOpts} from "./Writable";
import {Component} from "./Component";

/**
 * The end of a pipeline
 */
export class Sink extends Writable(Component) {
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    constructor(opts: WritableOpts) {
        super(opts);

        console.log("sink!");
    }
}
