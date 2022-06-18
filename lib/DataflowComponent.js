const DataflowChunk = require("./DataflowChunk");
const {TransformStream, WritableStream} = require("node:stream/web");
const DataflowTee = require("./DataflowTee");

module.exports = class DataflowComponent {
    constructor(cfg = {}) {
        this.name = cfg.name || "<unknown>";
    }

    pipe(dst) {
        if (dst instanceof Promise) {
            throw new TypeError("Attempting to pipe to a Promise. Did you accidentally try to pipe out of a DataflowSink?");
        }

        if (Array.isArray(dst)) {
            if (dst.length === 1) {
                // be nice if they called with an array with only one member
                dst = dst[0];
            } else {
                let t = new DataflowTee({
                    src: this,
                    dst,
                });
                return t.pipeAll();
            }
        }

        dst.source = this;
        this.dest = dst;
        if (dst.writableStream instanceof WritableStream) {
            dst.pipePromise = this.readableStream.pipeTo(dst.writableStream);
            return dst.pipePromise;
        } else if (dst.transformStream instanceof TransformStream) {
            this.readableStream.pipeThrough(dst.transformStream);
            return dst;
        }

        throw new TypeError("unsupported pipe destination type");
    }

    async send(controller, data) {
        if (Array.isArray(data)) {
            data.forEach((d) => {
                d = new DataflowChunk({d});
                controller.enqueue(d);
            });
        } else {
            data = new DataflowChunk({data});
            controller.enqueue(data);
        }
    }
};
