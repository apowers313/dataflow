const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowRoutedOutput, DataflowThrough, DataflowSink} = require("../index.js");
const {TestRoute} = require("./helpers/helpers.js");
// multiInputFifoOutput
describe("DataflowRoutedOutput", function() {
    it("is function", function() {
        assert.isFunction(DataflowRoutedOutput);
    });

    describe("route", function() {
        it("throws if data is not DataflowChannelizedChunks");

        it.only("to two sinks", async function() {
            const testSource = new TestRoute({numOutputs: 2});
            const writeSpy1 = spy();
            const sink1 = new DataflowSink({push: writeSpy1});
            const writeSpy2 = spy();
            const sink2 = new DataflowSink({push: writeSpy2});

            // let t = new DataflowRoutedOutput({src: testSource, numDests: 2});
            testSource.output.channels[0].pipe(sink1);
            testSource.output.channels[1].pipe(sink2);
            await testSource.complete();

            console.log("writeSpy1", writeSpy1.args);
            console.log("writeSpy2", writeSpy2.args);

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
            let t = new DataflowRoutedOutput({src: testSource, dst: [sink1, sink2, sink3]});
            await t.pipeAll();
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

            let t = new DataflowRoutedOutput({src: testSource, dst: [sink1, thru]});
            // await Promise.all([t.pipeAll(), p]);
            await t.pipeAll();

            assert.strictEqual(writeSpy1.callCount, 11);
            assert.deepEqual(writeSpy1.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy1.lastCall.args[0], {count: 10});
            assert.strictEqual(writeSpy2.callCount, 11);
            assert.deepEqual(writeSpy2.firstCall.args[0], {count: 0});
            assert.deepEqual(writeSpy2.lastCall.args[0], {count: 10});
        });

        it("errors if no channels piped");
        it("errors if sending to a non-piped channel");
    });

    it("routes the input to the correct output");

    it("copies chunks");
});
