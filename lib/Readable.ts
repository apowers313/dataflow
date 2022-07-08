import {Component, ComponentOpts} from "./Component";
import type {WritableType} from "./Writable";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor<T = Record<any, any>> = new (... args: any[]) => T;

export interface ReadableOpts extends ComponentOpts {
    numOutputs?: number;
}

/**
 * Applies the Reader mixin to a base class
 *
 * @param Base The base class the mixin will be applied to
 * @returns Reader
 */
export function Readable<TBase extends Constructor<Component>>(Base: TBase) {
    /**
     * Creates a stream that can be read from
     */
    return class Reader extends Base {
        readonly isReadable = true;
        numOutputs = 1;
        outputs: Array<OutputChannel>;
        // TODO: this should be a getter
        dests: Array<ReadableType> = [];

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        constructor(... args: any[]) {
            super(... args);

            const opts: ReadableOpts = args[0] ?? {};

            this.numOutputs = opts.numOutputs ?? this.numOutputs;
            this.outputs = new Array(this.numOutputs).map((o: unknown, idx: number) => new OutputChannel({chNum: idx, parent: this}));
        }
    };
}

export interface OutputChannelOpts {
    chNum: number;
    parent: ReadableType;
}

/**
 * An output stream
 */
export class OutputChannel {
    chNum: number;
    parent: ReadableType;
    dests: Array<WritableType> = [];

    /**
     * Creates a new output stream
     *
     * @param opt options for the new output stream
     */
    constructor(opt: OutputChannelOpts) {
        this.chNum = opt.chNum;
        this.parent = opt.parent;
    }

    pipe(dst: WritableType | Array<WritableType>) {
        if (!Array.isArray(dst)) {
            dst = [dst];
        }

        dst.forEach((d) => {
            d.addSource(this.parent);
        });

        this.dests = this.dests.concat(dst);
    }
}

export class ReadableType extends Readable(Component) {}
