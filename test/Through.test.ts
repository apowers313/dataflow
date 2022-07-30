import {Chunk, ChunkCollection, DataflowEnd, DataflowStart, MetadataChunk, Sink, Source, Through} from "../index";
import {TestMetadata, TestSource, through} from "./helpers/helpers";
import {assert} from "chai";
import {spy} from "sinon";

describe("Through", function() {
    it("is a class", function() {
        assert.isFunction(Through);
        const t = new Through({name: "foo", numChannels: 4, through});
        assert.instanceOf(t, Through);
        assert.strictEqual(t.name, "foo");
        assert.strictEqual(t.numChannels, 4);
    });

    it("is readable", function() {
        const t = new Through({through});
        assert.isTrue(t.isReadable);
    });

    it("is writable", function() {
        const t = new Through({through});
        assert.isTrue(t.isWritable);
    });

    describe("through", function() {
        it("passes data", async function() {
            const src = new TestSource();
            const thru = new Through({
                name: "through",
                through: async(chunk, method): Promise<void> => {
                    if (!chunk.isData()) {
                        throw new Error("expected data");
                    }

                    (chunk.data.count as number) *= 2;
                    await method.send(0, Chunk.create({type: "data", data: chunk.data}));
                },
            });
            const writeSpy = spy();
            const startSpy = spy();
            const closeSpy = spy();
            const abortSpy = spy();
            const sink = new Sink({
                name: "sink",
                push: writeSpy,
                writeStart: startSpy,
                writeClose: closeSpy,
                writeAbort: abortSpy,
            });

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);
            await src.complete();

            assert.strictEqual(startSpy.callCount, 1);
            assert.strictEqual(writeSpy.callCount, 11);
            assert.deepEqual(writeSpy.firstCall.args[0], {type: "data", data: {count: 0}});
            assert.deepEqual(writeSpy.lastCall.args[0], {type: "data", data: {count: 20}});
            assert.strictEqual(closeSpy.callCount, 1);
            assert.strictEqual(abortSpy.callCount, 0);
        });
    });

    it("passes through error", async function() {
        let pullCount = 0;
        const src = new Source({
            pull: async(methods): Promise<void> => {
                switch (pullCount) {
                case 0: {
                    await methods.send(0, Chunk.create({type: "data", data: {milesIsMad: true}}));
                    break;
                }
                case 1: {
                    const chunk = Chunk.create({type: "error", error: new Error("FUBAR"), data: null});
                    const cc = ChunkCollection.broadcast(chunk, this.numChannels);
                    cc.add(0, chunk);
                    await methods.sendMulti(cc);
                    break;
                }
                case 2: {
                    await methods.send(0, Chunk.create({type: "data", data: {freeBeer: false}}));
                    break;
                }
                default: {
                    await methods.finished();
                    return;
                }
                }

                pullCount++;
            },
            numChannels: 2,
            name: "source",
        });
        const thruSpy = spy();
        const thru = new Through({
            through: async(data, methods): Promise<void> => {
                thruSpy(data);
                await methods.send(0, data);
            },
            name: "through",
        });
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy, writeAll: true, name: "sink"});
        src.channels[0].pipe(thru);
        thru.channels[0].pipe(sink);
        await src.complete();

        // thru
        assert.strictEqual(thruSpy.callCount, 2);
        const thru0 = thruSpy.args[0][0];
        assert.isTrue(thru0.isData());
        assert.deepEqual(thru0.data, {milesIsMad: true});

        const thru1 = thruSpy.args[1][0];
        assert.isTrue(thru1.isData());
        assert.deepEqual(thru1.data, {freeBeer: false});

        // sink
        assert.strictEqual(sinkSpy.callCount, 5);
        const chunk0 = sinkSpy.args[0][0];
        assert.isTrue(chunk0.isMetadata());
        assert.isTrue(chunk0.metadata.has(DataflowStart));

        const chunk1 = sinkSpy.args[1][0];
        assert.isTrue(chunk1.isData());
        assert.deepEqual(chunk1.data, {milesIsMad: true});

        const chunk2 = sinkSpy.args[2][0];
        assert.isTrue(chunk2.isError());
        assert.strictEqual(chunk2.error.message, "FUBAR");

        const chunk3 = sinkSpy.args[3][0];
        assert.isTrue(chunk3.isData());
        assert.deepEqual(chunk3.data, {freeBeer: false});

        const chunk4 = sinkSpy.args[4][0];
        assert.isTrue(chunk4.isMetadata());
        assert.isTrue(chunk4.metadata.has(DataflowEnd));
    });

    it("passes through metadata", async function() {
        let pullCount = 0;
        const src = new Source({
            pull: async(methods): Promise<void> => {
                switch (pullCount) {
                case 0: {
                    await methods.send(0, Chunk.create({type: "data", data: {milesIsMad: true}}));
                    break;
                }
                case 1: {
                    const chunk = Chunk.create({type: "metadata"}) as MetadataChunk;
                    chunk.metadata.add(new TestMetadata());
                    const cc = ChunkCollection.broadcast(chunk, this.numChannels);
                    cc.add(0, chunk);
                    await methods.sendMulti(cc);
                    break;
                }
                case 2: {
                    await methods.send(0, Chunk.create({type: "data", data: {freeBeer: false}}));
                    break;
                }
                default: {
                    await methods.finished();
                    return;
                }
                }

                pullCount++;
            },
            numChannels: 2,
            name: "source",
        });
        const thruSpy = spy();
        const thru = new Through({
            through: async(data, methods): Promise<void> => {
                thruSpy(data);
                await methods.send(0, data);
            },
            name: "through",
        });
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy, writeAll: true, name: "sink"});
        src.channels[0].pipe(thru);
        thru.channels[0].pipe(sink);
        await src.complete();

        // thru
        assert.strictEqual(thruSpy.callCount, 2);
        const thru0 = thruSpy.args[0][0];
        assert.isTrue(thru0.isData());
        assert.deepEqual(thru0.data, {milesIsMad: true});

        const thru1 = thruSpy.args[1][0];
        assert.isTrue(thru1.isData());
        assert.deepEqual(thru1.data, {freeBeer: false});

        // sink
        assert.strictEqual(sinkSpy.callCount, 5);
        const chunk0 = sinkSpy.args[0][0];
        assert.isTrue(chunk0.isMetadata());
        assert.isTrue(chunk0.metadata.has(DataflowStart));

        const chunk1 = sinkSpy.args[1][0];
        assert.isTrue(chunk1.isData());
        assert.deepEqual(chunk1.data, {milesIsMad: true});

        const chunk2 = sinkSpy.args[2][0];
        assert.isTrue(chunk2.isMetadata());
        assert.isTrue(chunk2.metadata.has(TestMetadata));

        const chunk3 = sinkSpy.args[3][0];
        assert.isTrue(chunk3.isData());
        assert.deepEqual(chunk3.data, {freeBeer: false});

        const chunk4 = sinkSpy.args[4][0];
        assert.isTrue(chunk4.isMetadata());
        assert.isTrue(chunk4.metadata.has(DataflowEnd));
    });

    it("catches error", async function() {
        let pullCount = 0;
        const src = new Source({
            pull: async(methods): Promise<void> => {
                switch (pullCount) {
                case 0: {
                    await methods.send(0, Chunk.create({type: "data", data: {milesIsMad: true}}));
                    break;
                }
                case 1: {
                    await methods.send(0, Chunk.create({type: "data", data: {badChunk: true}}));
                    break;
                }
                case 2: {
                    await methods.send(0, Chunk.create({type: "data", data: {freeBeer: false}}));
                    break;
                }
                default: {
                    await methods.finished();
                    return;
                }
                }

                pullCount++;
            },
            numChannels: 2,
            name: "source",
        });
        const thruSpy = spy();
        const thru = new Through({
            through: async(chunk, methods): Promise<void> => {
                thruSpy(chunk);
                if (chunk.isData() && chunk.data.badChunk) {
                    throw Error("fml");
                }

                await methods.send(0, chunk);
            },
            name: "through",
        });
        const sinkSpy = spy();
        const sink = new Sink({push: sinkSpy, writeAll: true, name: "sink"});
        src.channels[0].pipe(thru);
        thru.channels[0].pipe(sink);
        await src.complete();

        // thru
        assert.strictEqual(thruSpy.callCount, 3);
        const thru0 = thruSpy.args[0][0];
        assert.isTrue(thru0.isData());
        assert.deepEqual(thru0.data, {milesIsMad: true});

        const thru1 = thruSpy.args[1][0];
        assert.isTrue(thru1.isData());
        assert.deepEqual(thru1.data, {badChunk: true});

        const thru2 = thruSpy.args[2][0];
        assert.isTrue(thru2.isData());
        assert.deepEqual(thru2.data, {freeBeer: false});

        // sink
        assert.strictEqual(sinkSpy.callCount, 5);
        const chunk0 = sinkSpy.args[0][0];
        assert.isTrue(chunk0.isMetadata());
        assert.isTrue(chunk0.metadata.has(DataflowStart));

        const chunk1 = sinkSpy.args[1][0];
        assert.isTrue(chunk1.isData());
        assert.deepEqual(chunk1.data, {milesIsMad: true});

        const chunk2 = sinkSpy.args[2][0];
        assert.isTrue(chunk2.isError());
        assert.strictEqual(chunk2.error.message, "fml");
        assert.deepEqual(chunk2.data, {type: "data", data: {badChunk: true}});

        const chunk3 = sinkSpy.args[3][0];
        assert.isTrue(chunk3.isData());
        assert.deepEqual(chunk3.data, {freeBeer: false});

        const chunk4 = sinkSpy.args[4][0];
        assert.isTrue(chunk4.isMetadata());
        assert.isTrue(chunk4.metadata.has(DataflowEnd));
    });

    describe("catchAll", function() {
        it("catches errors and metadata", async function() {
            let pullCount = 0;
            const src = new Source({
                pull: async(methods): Promise<void> => {
                    switch (pullCount) {
                    case 0: {
                        await methods.send(0, Chunk.create({type: "data", data: {milesIsMad: true}}));
                        break;
                    }
                    case 1: {
                        const chunk = Chunk.create({type: "error", error: new Error("FUBAR"), data: null});
                        const cc = ChunkCollection.broadcast(chunk, this.numChannels);
                        cc.add(0, chunk);
                        await methods.sendMulti(cc);
                        break;
                    }
                    case 2: {
                        const chunk = Chunk.create({type: "metadata"}) as MetadataChunk;
                        chunk.metadata.add(new TestMetadata());
                        const cc = ChunkCollection.broadcast(chunk, this.numChannels);
                        cc.add(0, chunk);
                        await methods.sendMulti(cc);
                        break;
                    }
                    default: {
                        await methods.finished();
                        return;
                    }
                    }

                    pullCount++;
                },
                numChannels: 2,
                name: "source",
            });
            const thruSpy = spy();
            const thru = new Through({
                through: async(data, methods): Promise<void> => {
                    thruSpy(data);
                    await methods.send(0, data);
                },
                name: "through",
                catchAll: true,
            });
            const sinkSpy = spy();
            const sink = new Sink({push: sinkSpy, writeAll: true, name: "sink"});
            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);
            await src.complete();

            // thru
            assert.strictEqual(thruSpy.callCount, 5);
            const thru0 = thruSpy.args[0][0];
            assert.isTrue(thru0.isMetadata());
            assert.isTrue(thru0.metadata.has(DataflowStart));

            const thru1 = thruSpy.args[1][0];
            assert.isTrue(thru1.isData());
            assert.deepEqual(thru1.data, {milesIsMad: true});

            const thru2 = thruSpy.args[2][0];
            assert.isTrue(thru2.isError());
            assert.strictEqual(thru2.error.message, "FUBAR");

            const thru3 = thruSpy.args[3][0];
            assert.isTrue(thru3.isMetadata());
            assert.isTrue(thru3.metadata.has(TestMetadata));

            const thru4 = thruSpy.args[4][0];
            assert.isTrue(thru4.isMetadata());
            assert.isTrue(thru4.metadata.has(DataflowEnd));

            // sink
            assert.strictEqual(sinkSpy.callCount, 5);
            const chunk0 = sinkSpy.args[0][0];
            assert.isTrue(chunk0.isMetadata());
            assert.isTrue(chunk0.metadata.has(DataflowStart));

            const chunk1 = sinkSpy.args[1][0];
            assert.isTrue(chunk1.isData());
            assert.deepEqual(chunk1.data, {milesIsMad: true});

            const chunk2 = sinkSpy.args[2][0];
            assert.isTrue(chunk2.isError());
            assert.strictEqual(chunk2.error.message, "FUBAR");

            const chunk3 = sinkSpy.args[3][0];
            assert.isTrue(chunk3.isMetadata());
            assert.isTrue(chunk3.metadata.has(TestMetadata));

            const chunk4 = sinkSpy.args[4][0];
            assert.isTrue(chunk4.isMetadata());
            assert.isTrue(chunk4.metadata.has(DataflowEnd));
        });
    });
});
