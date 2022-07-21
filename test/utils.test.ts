import {Sink, Source, Through, utils} from "../index";
const {isReadable, isWritable, walkStream, DeferredPromise} = utils;
import {assert} from "chai";
import {spy} from "sinon";
import {resolve} from "node:path";

// eslint-disable-next-line @typescript-eslint/no-empty-function
async function pull(): Promise<void> { }
// eslint-disable-next-line @typescript-eslint/no-empty-function
async function push(): Promise<void> {}

describe("utils", function() {
    describe("isReadable", function() {
        it("is function", function() {
            assert.isFunction(isReadable);
        });

        it("correctly identifies components", function() {
            assert.isTrue(isReadable(new Source({pull})));
            assert.isTrue(isReadable(new Through()));
            assert.isFalse(isReadable(new Sink({push})));
        });
    });

    describe("isWritable", function() {
        it("is function", function() {
            assert.isFunction(isWritable);
        });

        it("correctly identifies components", function() {
            assert.isFalse(isWritable(new Source({pull})));
            assert.isTrue(isWritable(new Through()));
            assert.isTrue(isWritable(new Sink({push})));
        });
    });

    describe("walkStream", function() {
        it("walks simple stream from source", function() {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            const src = new Source({name: "source", pull});
            const thru = new Through({name: "through"});
            const sink = new Sink({name: "sink", push});

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);

            const walkSpy = spy();
            walkStream(src, walkSpy);

            console.log("walkSpy.args", walkSpy.args);
            assert.deepEqual(walkSpy.args, [[src], [thru], [sink]]);
        });

        it("walks simple stream from middle", function() {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            const src = new Source({name: "source", pull});
            const thru = new Through({name: "through"});
            const sink = new Sink({name: "sink", push});

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);

            const walkSpy = spy();
            walkStream(thru, walkSpy);

            // console.log("walkSpy.args", walkSpy.args);
            assert.deepEqual(walkSpy.args, [[thru], [sink], [src]]);
        });

        it("walks simple stream from sink", function() {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            const src = new Source({name: "source", pull});
            const thru = new Through({name: "through"});
            const sink = new Sink({name: "sink", push});

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);

            const walkSpy = spy();
            walkStream(sink, walkSpy);

            console.log("walkSpy.args", walkSpy.args);
            assert.deepEqual(walkSpy.args, [[sink], [thru], [src]]);
        });
    });

    describe("DeferredPromise", function() {
        it("exists", function() {
            assert.isFunction(DeferredPromise);
        });

        it("immediately has properties", function() {
            const dp = new DeferredPromise();
            assert.instanceOf(dp.promise, Promise);
            assert.isFunction(dp.resolve);
            assert.isFunction(dp.reject);
        });

        it("resolves", async function() {
            const dp = new DeferredPromise();
            const resolveSpy = spy();
            const rejectSpy = spy();

            dp.resolve("foo");
            await dp.promise.then(resolveSpy).catch(rejectSpy);

            assert.strictEqual(resolveSpy.callCount, 1);
            assert.strictEqual(rejectSpy.callCount, 0);
            assert.deepEqual(resolveSpy.args, [["foo"]]);
        });

        it("rejects", async function() {
            const dp = new DeferredPromise();
            const resolveSpy = spy();
            const rejectSpy = spy();

            dp.reject(new Error("foo"));
            await dp.promise.then(resolveSpy).catch(rejectSpy);

            assert.strictEqual(resolveSpy.callCount, 0);
            assert.strictEqual(rejectSpy.callCount, 1);
            console.log("args", rejectSpy.args);
            assert.strictEqual(rejectSpy.args[0][0].message, "foo");
        });
    });
});
