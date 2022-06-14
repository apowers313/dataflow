const DataflowChunk = require("./DataflowChunk");
const {TransformStream, WritableStream} = require("node:stream/web");

module.exports = class DataflowComponent {
    constructor(cfg = {}) {
        this.name = cfg.name || "<unknown>";
    }

    pipe(dst) {
        if (dst instanceof Promise) {
            throw new TypeError("Attempting to pipe to a Promise. Did you accidentally try to pipe out of a DataflowSink?");
        }

        // if (dst.writableStream instanceof WritableStream) {
        //     dst = dst.writableStream;
        // } else if (dst.transformStream instanceof TransformStream) {
        //     dst = dst.transformStream;
        // }

        // if (dst instanceof WritableStream) {
        //     let ret = this.readableStream.pipeTo(dst);
        //     console.log("pipe() for WritableStream returning:", ret);
        //     return ret;
        // }

        // if (dst instanceof TransformStream) {
        //     this.readableStream.pipeThrough(dst);
        //     console.log("pipe() for TransformStream returning:", dst);
        //     return dst;
        // }

        if (dst.writableStream instanceof WritableStream) {
            let ret = this.readableStream.pipeTo(dst.writableStream);
            return ret;
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
