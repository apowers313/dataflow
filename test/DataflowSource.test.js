const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowSource} = require("../index.js");
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

    it("catches errors");

    describe("pipe", function() {
        it("pipes to TransformStream and returns chainable");
        it("pipes to WritableStream and returns Promise");
    });

    describe("config", function() {
        it("throws if pull isn't specified");
        it("start");
        it("pull");
        it("close");
        it("abort");
    });
});
