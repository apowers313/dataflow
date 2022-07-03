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

function isRoute(o) {
    return (typeof o === "object") && (typeof o.output === "object") && (Array.isArray(o.output.channels));
}

function isMirror(o) {
    return (typeof o === "object") && (Array.isArray(this.dests));
}

function walkStream(o, cb) {
    if (typeof o !== "object") {
        throw new TypeError("walkStreams stumbled on a non-object argument");
    }

    cb(o);

    // end of stream, this branch of the recursion is done
    if (!isReadable(o)) {
        return;
    }

    // recurse next object
    if (isRoute(o)) {
        o.output.channels.filter((c) => !!c.dest).forEach((c) => walkStream(c.dest, cb));
    } else if (isMirror(o)) {
        o.dests.forEach((d) => walkStream(d, cb));
    } else if (typeof o.dest === "object") {
        walkStream(o.dest, cb);
    } else {
        throw new TypeError("make sure to pipe() all objects before calling walkStreams");
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
    isRoute,
    isMirror,
    getReadableStream,
    getWritableStream,
    walkStream,
    promiseState,
};
