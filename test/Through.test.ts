import {Chunk, Sink, Through} from "../index";
import {TestSource, through} from "./helpers/helpers";
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
});
