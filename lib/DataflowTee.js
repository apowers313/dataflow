const {TransformStream} = require("node:stream/web");
const {isReadable, isWritable, getReadableStream} = require("./utils");

module.exports = class DataflowTee {
    constructor(cfg = {}) {
        if (!isReadable(cfg.src)) {
            throw new TypeError("expected source to be a DataflowSource or DataflowThrough");
        }

        this.source = cfg.src;

        let {dst} = cfg;
        if (!Array.isArray(dst) || dst.length < 2) {
            throw new TypeError("expected dst to be an Array of at least length 2");
        }

        dst.forEach((d) => {
            if (!isWritable(d)) {
                throw new TypeError("expected all destinations to be writable");
            }
        });

        let tees = [];
        let curr = getReadableStream(this.source);
        for (let i = 0; i < dst.length - 1; i++) {
            let t = curr.tee();
            tees.push(t[0]);
            curr = t[1];
        }
        tees.push(curr);

        this.tees = tees;
        this.dest = dst;
    }

    async pipeAll() {
        let returns = [];

        this.source.dest = this;
        for (let i = 0; i < this.tees.length; i++) {
            let src = this.tees[i];
            let dst = this.dest[i];
            if (dst.transformStream instanceof TransformStream) {
                returns.push(src.pipeThrough(dst.transformStream));
            } else {
                let p = src.pipeTo(dst.writableStream);
                dst.pipePromise = p;
                returns.push(p);
            }
        }

        return returns;
    }
};
