import {Chunk, ChunkCollection, Sink, Source} from "../index";
import {TestSource} from "./helpers/helpers";
import {assert} from "chai";
import {spy} from "sinon";

// eslint-disable-next-line @typescript-eslint/no-empty-function
async function pull(): Promise<void> { }

describe("Source", function() {
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
                start: startSpy,
                close: closeSpy,
                abort: abortSpy,
            });

            // const testSink = new WritableStream({
            //     start: startSpy,
            //     write: writeSpy,
            //     close: closeSpy,
            //     abort: abortSpy,
            // });

            src.channels[0].pipe(sink);
            // await src.init();
            await src.complete();

            assert.strictEqual(startSpy.callCount, 1);
            assert.strictEqual(writeSpy.callCount, 11);
            console.log("+++ ARGS", writeSpy.args);
            assert.deepEqual(writeSpy.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy.lastCall.args[0], {count: 10});
            assert.strictEqual(closeSpy.callCount, 1);
            assert.strictEqual(abortSpy.callCount, 0);
        });
    });
});
