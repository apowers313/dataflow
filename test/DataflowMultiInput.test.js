const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowMultiInput, DataflowSink} = require("../index.js");
const {TestSource} = require("./helpers/helpers.js");
// multiInputFifoOutput
describe("DataflowMultiInput", function() {
    this.slow(250);

    it("is function", function() {
        assert.isFunction(DataflowMultiInput);
    });

    it("merges two streams", async function() {
        this.retries(4);

        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new DataflowSink({push: sinkSpy});
        const mi = new DataflowMultiInput({src: [src1, src2], dst: sink});
        await mi.pipeAll();
        assert.strictEqual(sinkSpy.callCount, 10);
        let args = sinkSpy.args.map((a) => a[0]);

        assert.deepEqual(args, [
            {count: 5},
            {count: 10},
            {count: 13},
            {count: 15},
            {count: 20},
            {count: 26},
            {count: 25},
            {count: 39},
            {count: 52},
            {count: 65},
        ]);
    });

    it("zipper merges two streams", async function() {
        this.retries(4);

        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new DataflowSink({push: sinkSpy});
        const mi = new DataflowMultiInput({src: [src1, src2], dst: sink, mode: "zipper"});
        await mi.pipeAll();
        assert.strictEqual(sinkSpy.callCount, 10);
        let args = sinkSpy.args.map((a) => a[0]);

        assert.deepEqual(args, [
            {count: 5},
            {count: 13},
            {count: 10},
            {count: 26},
            {count: 15},
            {count: 39},
            {count: 20},
            {count: 52},
            {count: 25},
            {count: 65},
        ]);
    });

    it("batch merges two streams", async function() {
        this.retries(4);

        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new DataflowSink({push: sinkSpy});
        const mi = new DataflowMultiInput({src: [src1, src2], dst: sink, mode: "batch"});
        await mi.pipeAll();

        let args = sinkSpy.args.map((a) => a[0]);

        assert.deepEqual(args, [
            {0: {count: 5}, 1: {count: 13}},
            {0: {count: 10}, 1: {count: 26}},
            {0: {count: 15}, 1: {count: 39}},
            {0: {count: 20}, 1: {count: 52}},
            {0: {count: 25}, 1: {count: 65}},
        ]);
    });
});
