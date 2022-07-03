const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowSink} = require("../index.js");
const {TestSource} = require("./helpers/helpers.js");

describe("DataflowSink", function() {
    it("is a class", function() {
        assert.isFunction(DataflowSink);
    });

    it("receives data", async function() {
        const testSource = new TestSource();
        const writeSpy = spy();
        const testSink = new DataflowSink({push: writeSpy});

        await testSource.pipe(testSink);

        assert.strictEqual(writeSpy.callCount, 11);
        assert.deepEqual(writeSpy.firstCall.args[0], {count: 0});
        assert.deepEqual(writeSpy.lastCall.args[0], {count: 10});
    });

    it("catches errors");

    it("ignores errors");

    it("ignores metadata");

    it("doesn't support pipe");

    it("adds to pendingPromises");

    describe("config", function() {
        it("throws on no push");
        it("pushes errors and metadata if 'allChunks' is true");
        it("start");
        it("push");
        it("close");
        it("abort");
    });
});
