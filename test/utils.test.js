const {assert} = require("chai");
const {spy} = require("sinon");
const {utils, DataflowSource, DataflowThrough, DataflowSink} = require("../index.js");
const {ReadableStream, WritableStream} = require("node:stream/web");
const {TestSource} = require("./helpers/helpers.js");
const {walkStream} = require("../lib/utils.js");
const {isReadable, isWritable, getReadableStream, getWritableStream, promiseState} = utils;

describe("utils", function() {
    describe("isReadable", function() {
        it("DataflowSource is true", function() {
            let src = new DataflowSource({pull: () => {}});
            assert.isTrue(isReadable(src));
        });
        it("DataflowThrough is true", function() {
            let thru = new DataflowThrough({through: () => {}});
            assert.isTrue(isReadable(thru));
        });
        it("DataflowSink is false", function() {
            let sink = new DataflowSink({push: () => {}});
            assert.isFalse(isReadable(sink));
        });
    });

    describe("isWritable", function() {
        it("DataflowSource is false", function() {
            let src = new DataflowSource({pull: () => {}});
            assert.isFalse(isWritable(src));
        });
        it("DataflowThrough is true", function() {
            let thru = new DataflowThrough({through: () => {}});
            assert.isTrue(isWritable(thru));
        });
        it("DataflowSink is true", function() {
            let sink = new DataflowSink({push: () => {}});
            assert.isTrue(isWritable(sink));
        });
    });

    describe("getReadableStream", function() {
        it("DataflowSource returns stream", function() {
            let src = new DataflowSource({pull: () => {}});
            let stream = getReadableStream(src);
            assert.instanceOf(stream, ReadableStream);
        });

        it("DataflowThrough returns stream", function() {
            let thru = new DataflowThrough({through: () => {}});
            let stream = getReadableStream(thru);
            assert.instanceOf(stream, ReadableStream);
        });

        it("DataflowSink returns null", function() {
            let sink = new DataflowSink({push: () => {}});
            let stream = getReadableStream(sink);
            assert.isNull(stream);
        });
    });

    describe("getWritableStream", function() {
        it("DataflowSource returns null", function() {
            let src = new DataflowSource({pull: () => {}});
            let stream = getWritableStream(src);
            assert.isNull(stream);
        });

        it("DataflowThrough returns stream", function() {
            let thru = new DataflowThrough({through: () => {}});
            let stream = getWritableStream(thru);
            assert.instanceOf(stream, WritableStream);
        });

        it("DataflowSink returns stream", function() {
            let sink = new DataflowSink({push: () => {}});
            let stream = getWritableStream(sink);
            assert.instanceOf(stream, WritableStream);
        });
    });

    describe("walkStreams", function() {
        it("iterates simple pipe", function() {
            let src = new TestSource();
            let thru1 = new DataflowThrough({through: (msg) => msg});
            let thru2 = new DataflowThrough({through: (msg) => msg});
            let sink = new DataflowSink({push: () => {}});
            src.pipe(thru1).pipe(thru2).pipe(sink);

            let cb = spy();
            walkStream(src, cb);
            assert.strictEqual(cb.callCount, 4);
            assert.strictEqual(cb.args[0][0], src);
            assert.strictEqual(cb.args[1][0], thru1);
            assert.strictEqual(cb.args[2][0], thru2);
            assert.strictEqual(cb.args[3][0], sink);
        });

        it("handles simple tee", function() {
            let src = new TestSource();
            let sink1 = new DataflowSink({push: () => {}});
            let sink2 = new DataflowSink({push: () => {}});
            let sink3 = new DataflowSink({push: () => {}});
            src.pipe([sink1, sink2, sink3]);

            let cb = spy();
            walkStream(src, cb);
            assert.strictEqual(cb.callCount, 4);
            assert.strictEqual(cb.args[0][0], src);
            assert.strictEqual(cb.args[1][0], sink1);
            assert.strictEqual(cb.args[2][0], sink2);
            assert.strictEqual(cb.args[3][0], sink3);
        });

        it("walks multi-input");
        it("walks channelized output");
        it("walks mirrored and channelized output");

        it("iterates a complex tree", function() {
            let src = new TestSource();
            let thru1 = new DataflowThrough({through: (msg) => msg, name: "thru1"});
            let thru2 = new DataflowThrough({through: (msg) => msg, name: "thru2"});
            let thru3 = new DataflowThrough({through: (msg) => msg, name: "thru3"});
            let sink1 = new DataflowSink({push: () => {}, name: "sink1"});
            let sink2 = new DataflowSink({push: () => {}, name: "sink2"});
            let sink3 = new DataflowSink({push: () => {}, name: "sink3"});
            let sink4 = new DataflowSink({push: () => {}, name: "sink4"});
            src.pipe([sink1, thru1, thru2]);
            thru1.pipe(sink2);
            thru2.pipe([thru3, sink3]);
            thru3.pipe(sink4);

            let cb = spy();
            walkStream(src, cb);
            assert.strictEqual(cb.callCount, 8);
            assert.strictEqual(cb.args[0][0], src);
            assert.strictEqual(cb.args[1][0], sink1);
            assert.strictEqual(cb.args[2][0], thru1);
            assert.strictEqual(cb.args[3][0], sink2);
            assert.strictEqual(cb.args[4][0], thru2);
            assert.strictEqual(cb.args[5][0], thru3);
            assert.strictEqual(cb.args[6][0], sink4);
            assert.strictEqual(cb.args[7][0], sink3);
        });

        it("iterates a tree with multiple sources");
    });

    describe("promiseState", function() {
        it("resolves", async function() {
            const p = Promise.resolve();
            const ret = await promiseState(p);
            assert.strictEqual(ret, "fulfilled");
        });

        it("rejects", async function() {
            const p = Promise.reject();
            const ret = await promiseState(p);
            assert.strictEqual(ret, "rejected");
        });

        it("pending", async function() {
            const p = new Promise(() => {});
            const ret = await promiseState(p);
            assert.strictEqual(ret, "pending");
        });
    });
});
