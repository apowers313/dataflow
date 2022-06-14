const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowThrough, DataflowSink} = require("../index.js");
const {WritableStream} = require("node:stream/web");
const {TestSource} = require("./helpers/helpers.js");

describe("DataflowThrough", function() {
    it("is a class", function() {
        assert.isFunction(DataflowThrough);
    });

    it("passes through data", async function() {
        let testSource = new TestSource();
        const sinkWriteSpy = spy();
        let testSink = new DataflowSink({
            name: "test-sink",
            push: sinkWriteSpy,
        });
        const throughStartSpy = spy();
        const throughFlushSpy = spy();
        const throughSpy = spy(function(data) {
            if (data.count % 2) {
                return data;
            }

            return null;
        });
        let testThrough = new DataflowThrough({
            start: throughStartSpy,
            through: throughSpy,
            flush: throughFlushSpy,
            name: "test-through",
        });

        await testSource
            .pipe(testThrough)
            .pipe(testSink);

        assert.strictEqual(throughStartSpy.callCount, 1);
        assert.strictEqual(sinkWriteSpy.callCount, 5);
        assert.deepEqual(sinkWriteSpy.firstCall.args[0], {count: 1});
        assert.deepEqual(sinkWriteSpy.lastCall.args[0], {count: 9});
        assert.strictEqual(throughFlushSpy.callCount, 1);
    });
});
