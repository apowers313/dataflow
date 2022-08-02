import {Chunk, ChunkCollection, Sink} from "../index";
import {TestSource, push} from "./helpers/helpers";
import {assert} from "chai";
import {spy} from "sinon";

describe("Sink", function() {
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
        this.slow(250);
        this.retries(4);

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
        this.slow(250);
        this.retries(4);

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
        this.slow(250);
        this.retries(4);

        const src1 = new TestSource({delay: 5, countBy: 5, sendNum: 5});
        const src2 = new TestSource({delay: 13, countBy: 13, sendNum: 5});
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy, mode: "batch"});

        src1.channels[0].pipe(sink);
        src2.channels[0].pipe(sink);
        await src1.complete();

        const args = sinkSpy.args.map((a) => a[0]);

        const ch0results = [5, 10, 15, 20, 25];
        const ch1results = [13, 26, 39, 52, 65];
        args.forEach((arg, idx) => {
            if (!(arg instanceof ChunkCollection)) {
                throw new Error("expected all args to be a chunk collection");
            }

            let chunk = Chunk.create({type: "data", data: {count: ch0results[idx]}});
            assert.deepEqual(arg.get(0), chunk);
            chunk = Chunk.create({type: "data", data: {count: ch1results[idx]}});
            assert.deepEqual(arg.get(1), chunk);
        });
    });

    describe("writeAll", function() {
        it("catches metadata", async function() {
            const src = new TestSource();
            const sinkSpy = spy();
            const sink = new Sink({push: sinkSpy, writeAll: true});
            src.channels[0].pipe(sink);
            // await src.init();
            await src.complete();

            assert.strictEqual(sinkSpy.callCount, 13);
            const chunk0 = sinkSpy.args[0][0];
            assert.isTrue(chunk0.isMetadata());
            assert.isTrue(chunk0.metadata.has("dataflow", "start"));
            assert.deepEqual(sinkSpy.args[1][0], {type: "data", data: {count: 0}});
            assert.deepEqual(sinkSpy.args[11][0], {type: "data", data: {count: 10}});
            const chunk12 = sinkSpy.args[12][0];
            assert.isTrue(chunk12.isMetadata());
            assert.isTrue(chunk12.metadata.has("dataflow", "end"));
        });
        it("catches errors");
    });
});
