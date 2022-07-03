const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowMirroredOutput, DataflowThrough, DataflowSink} = require("../index.js");
const {TestSource} = require("./helpers/helpers.js");
// multiInputFifoOutput
describe("DataflowMirroredOutput", function() {
    it("is function", function() {
        assert.isFunction(DataflowMirroredOutput);
    });

    describe("mirror", function() {
        it("to two sinks", async function() {
            const testSource = new TestSource();
            const writeSpy1 = spy();
            const sink1 = new DataflowSink({push: writeSpy1});
            const writeSpy2 = spy();
            const sink2 = new DataflowSink({push: writeSpy2});

            let t = new DataflowMirroredOutput({src: testSource, dst: [sink1, sink2]});
            await t.runPipe();
            // await testSource.complete();

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy1.lastCall.args[0], {count: 10});
            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy2.lastCall.args[0], {count: 10});
        });

        it("to three sinks", async function() {
            const testSource = new TestSource();
            const writeSpy1 = spy();
            const sink1 = new DataflowSink({push: writeSpy1});
            const writeSpy2 = spy();
            const sink2 = new DataflowSink({push: writeSpy2});
            const writeSpy3 = spy();
            const sink3 = new DataflowSink({push: writeSpy3});
            let t = new DataflowMirroredOutput({src: testSource, dst: [sink1, sink2, sink3]});
            await t.runPipe();
            // await testSource.complete();

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy1.lastCall.args[0], {count: 10});
            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy2.lastCall.args[0], {count: 10});
            assert.strictEqual(writeSpy3.callCount, 11);
            assert.deepEqual(writeSpy3.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy3.lastCall.args[0], {count: 10});
        });

        it("to a through and a sink", async function() {
            const testSource = new TestSource();
            const writeSpy1 = spy();
            const sink1 = new DataflowSink({push: writeSpy1});
            const thru = new DataflowThrough({through: (msg) => msg});
            const writeSpy2 = spy();
            const sink2 = new DataflowSink({push: writeSpy2});
            let p = thru.pipe(sink2);

            let t = new DataflowMirroredOutput({src: testSource, dst: [sink1, thru]});
            // await Promise.all([t.runPipe(), p]);
            await t.runPipe();

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy1.lastCall.args[0], {count: 10});
            console.log("writeSpy2 args", writeSpy2.args);
            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy2.lastCall.args[0], {count: 10});
        });
    });

    it("copies chunks");

    it("adds to pendingPromises");
});
