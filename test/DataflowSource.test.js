const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowSource, DataflowThrough, DataflowSink} = require("../index.js");
const {WritableStream} = require("node:stream/web");
const {TestSource} = require("./helpers/helpers.js");

describe("DataflowSource", function() {
    it("is a class", function() {
        assert.isFunction(DataflowSource);
    });

    it("writes data", async function() {
        let testSource = new TestSource();
        const writeSpy = spy();
        const startSpy = spy();
        const closeSpy = spy();
        const abortSpy = spy();

        let testSink = new WritableStream({
            start: startSpy,
            write: writeSpy,
            close: closeSpy,
            abort: abortSpy,
        });

        await testSource.readableStream.pipeTo(testSink);

        assert.strictEqual(startSpy.callCount, 1);
        assert.strictEqual(writeSpy.callCount, 11);
        assert.deepEqual(writeSpy.firstCall.args[0], {data: {count: 0}, type: "data"});
        assert.deepEqual(writeSpy.lastCall.args[0], {data: {count: 10}, type: "data"});
        assert.strictEqual(closeSpy.callCount, 1);
        assert.strictEqual(abortSpy.callCount, 0);
    });

    it("writes to all outputs", async function() {
        const src = new DataflowSource({
            numOutputs: 3,
            pull: (methods) => {
                let count = 0;
                if (count > 10) {
                    return methods.finished();
                }

                return count.multiOutputSend();
            },
        });
        const sinkSpy1 = spy();
        const sink1 = new DataflowSink({push: sinkSpy1});
        const sinkSpy2 = spy();
        const sink2 = new DataflowSink({push: sinkSpy2});
        const sinkSpy3 = spy();
        const sink3 = new DataflowSink({push: sinkSpy3});

        src.pipeFromChannel(0, sink1);
        src.pipeFromChannel(1, sink2);
        src.pipeFromChannel(2, sink3);
        await src.complete();

        assert.strictEqual(sink1.callCount, 0);
        assert.strictEqual(sink2.callCount, 11);
        assert.strictEqual(sink3.callCount, 0);
    });
    it("writes to multiple outputs");
    it("writes to a single output");
    it("writes even with some outputs not piped to");

    it("catches errors");

    describe("pipe", function() {
        it("pipes to TransformStream and returns chainable");
        it("pipes to WritableStream and returns Promise");
    });

    describe("complete", function() {
        it("handles simple tee", async function() {
            let src = new TestSource();
            let sink1 = new DataflowSink({push: () => {}, name: "sink1"});
            let sink2 = new DataflowSink({push: () => {}, name: "sink2"});
            let sink3 = new DataflowSink({push: () => {}, name: "sink3"});
            src.pipe([sink1, sink2, sink3]);
            await src.complete();
        });

        it("iterates a complex tree", async function() {
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
            await src.complete();
        });
    });

    describe("config", function() {
        it("throws if pull isn't specified");
        it("start");
        it("pull");
        it("close");
        it("abort");
    });
});
