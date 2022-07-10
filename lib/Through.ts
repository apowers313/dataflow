import {Readable, ReadableOpts} from "./Readable";
import {Writable, WritableOpts} from "./Writable";
import {Component} from "./Component";

interface ThroughOpts extends Omit<ReadableOpts, "pull">, Omit<WritableOpts, "push"> {}

/**
 * A component that reads data from a input, acts on it, and then passes it to an output
 */
export class Through extends Writable(Readable(Component)) {
    /**
     * Creates a new through stream
     *
     * @param opts options for the new through stream
     */
    constructor(opts: ThroughOpts = {}) {
        super(opts);
    }
}
