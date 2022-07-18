import {Sink, Source} from "../index";
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

            src.outputs[0].pipe(sink);
            await src.init();
            await src.complete();

            assert.strictEqual(startSpy.callCount, 1);
            assert.strictEqual(writeSpy.callCount, 11);
            assert.deepEqual(writeSpy.firstCall.args[0], {data: {count: 0}, type: "data"});
            assert.deepEqual(writeSpy.lastCall.args[0], {data: {count: 10}, type: "data"});
            assert.strictEqual(closeSpy.callCount, 1);
            assert.strictEqual(abortSpy.callCount, 0);
        });
    });
});
