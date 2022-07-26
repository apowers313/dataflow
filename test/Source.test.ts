import {Chunk, ChunkCollection, Sink, Source, Through} from "../index";
import {TestRoute, TestSource, pull, push, through} from "./helpers/helpers";
import {assert} from "chai";
import {spy} from "sinon";

describe("Source", function() {
    this.slow(250);
    this.retries(4);

    it("is a class", function() {
        assert.isFunction(Source);
        const s = new Source({pull});
        assert.instanceOf(s, Source);
    });

    it("is readable", function() {
        const s = new Source({pull});
        assert.isTrue(s.isReadable);
    });

    it("is not writable", function() {
        const s = new Source({pull});
        assert.isFalse(s.isWritable);
    });

    describe("readFor", function() {
        it("reads once", async function() {
            const src = new Source({
                pull: async(methods): Promise<void> => {
                    console.log("pull");

                    const data = Chunk.create({type: "data", data: {foo: "bar"}});
                    await methods.send(0, data);
                },
            });
            src.channels[0].pipe(new Sink({push: spy()}));
            assert.strictEqual(src.channels[0].outputs.length, 1);
            const chunk = await src.channels[0].outputs[0].read();
            if (!chunk.isData()) {
                throw new Error("expected data chunk");
            }

            assert.deepEqual(chunk.data, {foo: "bar"});
        });

        it("reads twice", async function() {
            const src = new Source({
                pull: async(methods): Promise<void> => {
                    console.log("pull");

                    const data = Chunk.create({type: "data", data: {foo: "bar"}});
                    await methods.send(0, data);
                },
            });
            src.channels[0].pipe([new Sink({push: spy()}), new Sink({push: spy()})]);
            assert.strictEqual(src.channels[0].outputs.length, 2);
            const p1 = src.channels[0].outputs[0].read();
            const p2 = src.channels[0].outputs[1].read();
            const [chunk1, chunk2] = await Promise.all([p1, p2]);

            if (!chunk1.isData() || !chunk2.isData()) {
                throw new Error("expected data chunks");
            }

            assert.deepEqual(chunk1.data, {foo: "bar"});
            assert.deepEqual(chunk2.data, {foo: "bar"});
        });

        it("reads across channels", async function() {
            const src = new Source({
                pull: async(methods): Promise<void> => {
                    console.log("pull");

                    const data = Chunk.create({type: "data", data: {foo: "bar"}});
                    const cc = new ChunkCollection();
                    cc.add(0, data);
                    cc.add(1, data);
                    cc.add(2, data);
                    await methods.sendMulti(cc);
                },
                numChannels: 3,
            });
            src.channels[0].pipe(new Sink({push: spy()}));
            src.channels[1].pipe(new Sink({push: spy()}));
            src.channels[2].pipe(new Sink({push: spy()}));
            assert.strictEqual(src.channels[0].outputs.length, 1);
            assert.strictEqual(src.channels[1].outputs.length, 1);
            assert.strictEqual(src.channels[2].outputs.length, 1);
            const p1 = src.channels[0].outputs[0].read();
            const p2 = src.channels[1].outputs[0].read();
            const p3 = src.channels[2].outputs[0].read();
            const [chunk1, chunk2, chunk3] = await Promise.all([p1, p2, p3]);

            if (!chunk1.isData() || !chunk2.isData() || !chunk3.isData()) {
                throw new Error("expected data chunks");
            }

            assert.deepEqual(chunk1.data, {foo: "bar"});
            assert.deepEqual(chunk2.data, {foo: "bar"});
            assert.deepEqual(chunk3.data, {foo: "bar"});
        });
    });

    describe("pipe", function() {
        it("writes data", async function() {
            const src = new TestSource();
            const writeSpy = spy();
            const startSpy = spy();
            const closeSpy = spy();
            const abortSpy = spy();
            const sink = new Sink({
                push: writeSpy,
                writeStart: startSpy,
                writeClose: closeSpy,
                writeAbort: abortSpy,
            });

            src.channels[0].pipe(sink);
            // await src.init();
            await src.complete();

            assert.strictEqual(startSpy.callCount, 1);
            assert.strictEqual(writeSpy.callCount, 11);
            assert.deepEqual(writeSpy.firstCall.args[0], {type: "data", data: {count: 0}});
            assert.deepEqual(writeSpy.lastCall.args[0], {type: "data", data: {count: 10}});
            assert.strictEqual(closeSpy.callCount, 1);
            assert.strictEqual(abortSpy.callCount, 0);
        });
    });

    describe("complete", function() {
        it("handles simple tee", async function() {
            const src = new TestSource();
            const sink1 = new Sink({push, name: "sink1"});
            const sink2 = new Sink({push, name: "sink2"});
            const sink3 = new Sink({push, name: "sink3"});
            src.channels[0].pipe([sink1, sink2, sink3]);
            await src.complete();
        });

        it("iterates a complex tree", async function() {
            const src = new TestSource();
            const thru1 = new Through({through, name: "thru1"});
            const thru2 = new Through({through, name: "thru2"});
            const thru3 = new Through({through, name: "thru3"});
            const sinkSpy1 = spy();
            const sink1 = new Sink({push: sinkSpy1, name: "sink1"});
            const sinkSpy2 = spy();
            const sink2 = new Sink({push: sinkSpy2, name: "sink2"});
            const sinkSpy3 = spy();
            const sink3 = new Sink({push: sinkSpy3, name: "sink3"});
            const sinkSpy4 = spy();
            const sink4 = new Sink({push: sinkSpy4, name: "sink4"});
            src.channels[0].pipe([sink1, thru1, thru2]);
            thru1.channels[0].pipe(sink2);
            thru2.channels[0].pipe([thru3, sink3]);
            thru3.channels[0].pipe(sink4);
            await src.complete();

            // TODO: test results
            assert.strictEqual(sinkSpy1.callCount, 11);
            assert.strictEqual(sinkSpy2.callCount, 11);
            assert.strictEqual(sinkSpy3.callCount, 11);
            assert.strictEqual(sinkSpy4.callCount, 11);
        });
    });

    describe("route", function() {
        it("throws if data is not DataflowChannelizedChunks");

        it("to two sinks", async function() {
            const testSource = new TestRoute({numChannels: 2, outputType: "broadcast"});
            const writeSpy1 = spy();
            const sink1 = new Sink({push: writeSpy1});
            const writeSpy2 = spy();
            const sink2 = new Sink({push: writeSpy2});

            // let t = new DataflowRoutedOutput({src: testSource, numDests: 2});
            testSource.channels[0].pipe(sink1);
            testSource.channels[1].pipe(sink2);
            await testSource.complete();

            console.log("writeSpy1", writeSpy1.args);
            console.log("writeSpy2", writeSpy2.args);

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {type: "data", data: {count: "0-0"}});
            assert.deepEqual(writeSpy1.lastCall.args[0], {type: "data", data: {count: "0-10"}});

            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {type: "data", data: {count: "1-0"}});
            assert.deepEqual(writeSpy2.lastCall.args[0], {type: "data", data: {count: "1-10"}});
        });

        it("to three sinks", async function() {
            const testSource = new TestRoute({numChannels: 3, outputType: "broadcast"});
            const writeSpy1 = spy();
            const sink1 = new Sink({push: writeSpy1});
            const writeSpy2 = spy();
            const sink2 = new Sink({push: writeSpy2});
            const writeSpy3 = spy();
            const sink3 = new Sink({push: writeSpy3});

            testSource.channels[0].pipe(sink1);
            testSource.channels[1].pipe(sink2);
            testSource.channels[2].pipe(sink3);
            await testSource.complete();

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {type: "data", data: {count: "0-0"}});
            assert.deepEqual(writeSpy1.lastCall.args[0], {type: "data", data: {count: "0-10"}});
            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {type: "data", data: {count: "1-0"}});
            assert.deepEqual(writeSpy2.lastCall.args[0], {type: "data", data: {count: "1-10"}});
            assert.strictEqual(writeSpy3.callCount, 11);
            assert.deepEqual(writeSpy3.firstCall.args[0], {type: "data", data: {count: "2-0"}});
            assert.deepEqual(writeSpy3.lastCall.args[0], {type: "data", data: {count: "2-10"}});
        });

        it("to three sinks, two are mirrored", async function() {
            const testSource = new TestRoute({numChannels: 2, outputType: "broadcast"});
            const writeSpy1 = spy();
            const sink1 = new Sink({push: writeSpy1});
            const writeSpy2 = spy();
            const sink2 = new Sink({push: writeSpy2});
            const writeSpy3 = spy();
            const sink3 = new Sink({push: writeSpy3});

            testSource.channels[0].pipe(sink1);
            testSource.channels[0].pipe(sink2);
            testSource.channels[1].pipe(sink3);
            await testSource.complete();

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {type: "data", data: {count: "0-0"}});
            assert.deepEqual(writeSpy1.lastCall.args[0], {type: "data", data: {count: "0-10"}});
            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {type: "data", data: {count: "0-0"}});
            assert.deepEqual(writeSpy2.lastCall.args[0], {type: "data", data: {count: "0-10"}});
            assert.strictEqual(writeSpy3.callCount, 11);
            assert.deepEqual(writeSpy3.firstCall.args[0], {type: "data", data: {count: "1-0"}});
            assert.deepEqual(writeSpy3.lastCall.args[0], {type: "data", data: {count: "1-10"}});
        });

        it("errors if no channels piped");
        it("errors if sending to a non-piped channel");
        it("can send if a channel is un-piped");
        it("can send to mirrored output");
        it("errors if sendChunksToOutput is too large for number of channels");
        it("errors if sendChunksToOutput is too small for number of channels");
    });
});
