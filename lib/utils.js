const {ReadableStream, WritableStream} = require("node:stream/web");

function isReadable(o) {
    if (getReadableStream(o)) {
        return true;
    }

    return false;
}

function isWritable(o) {
    if (getWritableStream(o)) {
        return true;
    }

    return false;
}

function getReadableStream(o) {
    if (typeof o !== "object") {
        return null;
    }

    // DataflowSource
    if (o.readableStream instanceof ReadableStream) {
        return o.readableStream;
    }

    // DataflowThrough
    if ((typeof o.transformStream === "object") && (o.transformStream.readable instanceof ReadableStream)) {
        return o.transformStream.readable;
    }

    return null;
}

function getWritableStream(o) {
    if (typeof o !== "object") {
        return null;
    }

    // DataflowSink
    if (o.writableStream instanceof WritableStream) {
        return o.writableStream;
    }

    // DataflowThrough
    if ((typeof o.transformStream === "object") && (o.transformStream.writable instanceof WritableStream)) {
        return o.transformStream.writable;
    }

    return null;
}

function walkStream(o, cb) {
    if (typeof o !== "object") {
        throw new TypeError("walkStreams stumbled on a non-object argument");
    }

    // don't do callback on tee
    if (!Array.isArray(o.dest)) {
        cb(o);
    }

    if (o.writableStream instanceof WritableStream) {
        return;
    }

    if (typeof o.dest !== "object") {
        console.log("failure walking object:", o);
        throw new TypeError("make sure to pipe() all objects before calling walkStreams");
    }

    let dst = o.dest;
    if (Array.isArray(dst)) {
        dst.forEach((d) => {
            walkStream(d, cb);
        });
    } else {
        walkStream(dst, cb);
    }
}

function promiseState(p) {
    const t = {};
    return Promise.race([p, t])
        .then((v) => (v === t) ? "pending" : "fulfilled", () => "rejected");
}

module.exports = {
    isReadable,
    isWritable,
    getReadableStream,
    getWritableStream,
    walkStream,
    promiseState,
};
