import {TestSource, push} from "./helpers/helpers";
import {Sink} from "../index";
import {assert} from "chai";
import {spy} from "sinon";

describe("Sink", function() {
    this.slow(250);
    this.retries(4);

    it("is a class", function() {
        assert.isFunction(Sink);
    });

    it("is not readable", function() {
        const s = new Sink({push});
        assert.isFalse(s.isReadable);
    });

    it("is writable", function() {
        const s = new Sink({push});
        assert.isTrue(s.isWritable);
    });

    it("merges two streams", async function() {
        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy});

        src1.channels[0].pipe(sink);
        src2.channels[0].pipe(sink);
        await src1.complete();
        assert.strictEqual(sinkSpy.callCount, 10);
        const args = sinkSpy.args.map((a) => a[0]);

        assert.deepEqual(args, [
            {type: "data", data: {count: 5}},
            {type: "data", data: {count: 10}},
            {type: "data", data: {count: 13}},
            {type: "data", data: {count: 15}},
            {type: "data", data: {count: 20}},
            {type: "data", data: {count: 26}},
            {type: "data", data: {count: 25}},
            {type: "data", data: {count: 39}},
            {type: "data", data: {count: 52}},
            {type: "data", data: {count: 65}},
        ]);
    });

    it("zipper merges two streams", async function() {
        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy, mode: "zipper"});

        src1.channels[0].pipe(sink);
        src2.channels[0].pipe(sink);
        await src2.complete();
        assert.strictEqual(sinkSpy.callCount, 10);
        const args = sinkSpy.args.map((a) => a[0]);

        assert.deepEqual(args, [
            {type: "data", data: {count: 5}},
            {type: "data", data: {count: 13}},
            {type: "data", data: {count: 10}},
            {type: "data", data: {count: 26}},
            {type: "data", data: {count: 15}},
            {type: "data", data: {count: 39}},
            {type: "data", data: {count: 20}},
            {type: "data", data: {count: 52}},
            {type: "data", data: {count: 25}},
            {type: "data", data: {count: 65}},
        ]);
    });

    it("batch merges two streams", async function() {
        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy, mode: "batch"});

        src1.channels[0].pipe(sink);
        src2.channels[0].pipe(sink);
        await src1.complete();

        const args = sinkSpy.args.map((a) => a[0]);

        assert.deepEqual(args, [
            {type: "data", data: {0: {count: 5}, 1: {count: 13}}},
            {type: "data", data: {0: {count: 10}, 1: {count: 26}}},
            {type: "data", data: {0: {count: 15}, 1: {count: 39}}},
            {type: "data", data: {0: {count: 20}, 1: {count: 52}}},
            {type: "data", data: {0: {count: 25}, 1: {count: 65}}},
        ]);
    });
});
