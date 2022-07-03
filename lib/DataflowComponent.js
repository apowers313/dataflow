const DataflowChunk = require("./DataflowChunk");
const {TransformStream, WritableStream} = require("node:stream/web");
const DataflowMirroredOutput = require("./DataflowMirroredOutput");

const dataflowSymbol = Symbol();

module.exports = class DataflowComponent {
    constructor(cfg = {}) {
        this.name = cfg.name || "<unknown>";
        this.dataflow = dataflowSymbol;
        this.done = false;
        this.controller = null;
        this.methods = {};
        this.dataflowReadable = false;
        this.dataflowWritable = false;
        this.log = {
            error: console.error,
            warn: console.warn,
            info: console.info,
            debug: console.debug,
            trace: console.trace,
        };
        this.warnChannelOverflow = true;
        this.warnChannelUnderflow = true;
        this.pendingPromises = [];
    }

    static isDataflow(o) {
        if (typeof o === "object" &&
            o !== null &&
            o.dataflow === dataflowSymbol) {
            return true;
        }

        return false;
    }

    static readableMixin(o, methods = {}) {
        const sendMethod = methods.sendMethod || _genericSend;
        const linkMethod = methods.linkMethod || _genericLink;
        const pipeMethod = methods.pipeMethod || _genericPipe;
        o.methods = o.methods ?? {};

        console.log("plain mixin");
        o.pipe = pipe.bind(o, {linkMethod, pipeMethod});
        o.send = send.bind(o, {sendMethod});
        o.methods.send = send.bind(o, {sendMethod});
        o.methods.sendReady = sendReady.bind(o);
        o.dataflowReadable = true;
    }

    static writableMixin(o) {
        o.dataflowWritable = true;
    }
};

function pipe({linkMethod, pipeMethod}, dst) {
    if (dst instanceof Promise) {
        throw new TypeError("Attempting to pipe to a Promise. Did you accidentally try to pipe out of a DataflowSink?");
    }

    if (Array.isArray(dst)) {
        if (dst.length === 1) {
            // be nice if they called with an array with only one member
            dst = dst[0];
        } else {
            let t = new DataflowMirroredOutput({
                src: this,
                dst,
            });
            return t.pipeAll();
        }
    }

    linkMethod.call(this, dst);
    return pipeMethod.call(this, dst);
}

async function send({sendMethod}, ... args) {
    if (this.channels) {
        throw new Error("don't call send if multiple channels are configured, use sendToChannel instead");
    }

    for (let i = 0; i < args.length; i++) {
        await sendMethod.call(this, args[i]);
    }
}

async function sendReady() {
    console.log("sendReady not implemented");
}

function _genericLink(dst) {
    dst.source = this;
    this.dest = dst;
}

function _genericPipe(dst) {
    console.log("GENERIC PIPE", dst);
    if (dst.writableStream instanceof WritableStream) {
        let ret = this.readableStream.pipeTo(dst.writableStream);
        dst.pendingPromises.push(ret);
        return ret;
    } else if (dst.transformStream instanceof TransformStream) {
        this.readableStream.pipeThrough(dst.transformStream);
        return dst;
    }

    throw new TypeError("unsupported pipe destination type");
}

async function _genericSend(data) {
    console.trace();
    data = new DataflowChunk({data});
    this.controller.enqueue(data);
}
