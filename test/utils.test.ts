import {Sink, Source, Through, utils} from "../index";
const {isReadable, isWritable, walkStream, DeferredPromise, promiseState} = utils;
import {pull, push, through} from "./helpers/helpers";
import {assert} from "chai";
import {spy} from "sinon";

describe("utils", function() {
    describe("isReadable", function() {
        it("is function", function() {
            assert.isFunction(isReadable);
        });

        it("correctly identifies components", function() {
            assert.isTrue(isReadable(new Source({pull})));
            assert.isTrue(isReadable(new Through({through})));
            assert.isFalse(isReadable(new Sink({push})));
        });
    });

    describe("isWritable", function() {
        it("is function", function() {
            assert.isFunction(isWritable);
        });

        it("correctly identifies components", function() {
            assert.isFalse(isWritable(new Source({pull})));
            assert.isTrue(isWritable(new Through({through})));
            assert.isTrue(isWritable(new Sink({push})));
        });
    });

    describe("walkStream", function() {
        it("walks simple stream from source", function() {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            const src = new Source({name: "source", pull});
            const thru = new Through({name: "through", through});
            const sink = new Sink({name: "sink", push});

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);

            const walkSpy = spy();
            walkStream(src, walkSpy);

            assert.deepEqual(walkSpy.args, [[src], [thru], [sink]]);
        });

        it("walks simple stream from middle", function() {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            const src = new Source({name: "source", pull});
            const thru = new Through({name: "through", through});
            const sink = new Sink({name: "sink", push});

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);

            const walkSpy = spy();
            walkStream(thru, walkSpy);

            assert.deepEqual(walkSpy.args, [[thru], [sink], [src]]);
        });

        it("walks simple stream from sink", function() {
            const src = new Source({name: "source", pull});
            const thru = new Through({name: "through", through});
            const sink = new Sink({name: "sink", push});

            src.channels[0].pipe(thru);
            thru.channels[0].pipe(sink);

            const walkSpy = spy();
            walkStream(sink, walkSpy);

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
            assert.strictEqual(rejectSpy.args[0][0].message, "foo");
        });
    });

    describe("promiseState", function() {
        it("resolves", async function() {
            const p = Promise.resolve();
            const ret = await promiseState(p);
            assert.strictEqual(ret, "fulfilled");
        });

        it("rejects", async function() {
            const p = Promise.reject();
            const ret = await promiseState(p);
            assert.strictEqual(ret, "rejected");
        });

        it("pending", async function() {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            const p = new Promise(() => {});
            const ret = await promiseState(p);
            assert.strictEqual(ret, "pending");
        });
    });
});
