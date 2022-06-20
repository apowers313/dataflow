const {assert} = require("chai");
const {spy} = require("sinon");
const {DataflowTee, DataflowSource, DataflowThrough, DataflowSink} = require("../index.js");
const {WritableStream} = require("node:stream/web");
const {TestSource} = require("./helpers/helpers.js");

describe("DataflowTee", function() {
    it("is a class", function() {
        assert.isFunction(DataflowTee);
    });

    it("throws on bad src");
    it("throws on non-array dst");
    it("throws on dst.length < 2");
    it("throws non-writable dst member");

    it("tees to two sinks", async function() {
        const testSource = new TestSource();
        const writeSpy1 = spy();
        const sink1 = new DataflowSink({push: writeSpy1});
        const writeSpy2 = spy();
        const sink2 = new DataflowSink({push: writeSpy2});

        let t = new DataflowTee({src: testSource, dst: [sink1, sink2]});
        t.pipeAll();
        await testSource.complete();

        assert.strictEqual(writeSpy1.callCount, 11);
        assert.deepEqual(writeSpy1.firstCall.args[0], {count: 0});
        assert.deepEqual(writeSpy1.lastCall.args[0], {count: 10});
        assert.strictEqual(writeSpy2.callCount, 11);
        assert.deepEqual(writeSpy2.firstCall.args[0], {count: 0});
        assert.deepEqual(writeSpy2.lastCall.args[0], {count: 10});
    });
    it("tees to three sinks", async function() {
        const testSource = new TestSource();
        const writeSpy1 = spy();
        const sink1 = new DataflowSink({push: writeSpy1});
        const writeSpy2 = spy();
        const sink2 = new DataflowSink({push: writeSpy2});
        const writeSpy3 = spy();
        const sink3 = new DataflowSink({push: writeSpy3});
        let t = new DataflowTee({src: testSource, dst: [sink1, sink2, sink3]});
        t.pipeAll();
        await testSource.complete();

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

    it("tees to a through and a sink", async function() {
        const testSource = new TestSource();
        const writeSpy1 = spy();
        const sink1 = new DataflowSink({push: writeSpy1});
        const thru = new DataflowThrough({through: (msg) => msg});
        const writeSpy2 = spy();
        const sink2 = new DataflowSink({push: writeSpy2});
        let p = thru.pipe(sink2);

        let t = new DataflowTee({src: testSource, dst: [sink1, thru]});
        await Promise.all([t.pipeAll(), p]);

        assert.strictEqual(writeSpy1.callCount, 11);
        assert.deepEqual(writeSpy1.firstCall.args[0], {count: 0});
        assert.deepEqual(writeSpy1.lastCall.args[0], {count: 10});
        assert.strictEqual(writeSpy2.callCount, 11);
        assert.deepEqual(writeSpy2.firstCall.args[0], {count: 0});
        assert.deepEqual(writeSpy2.lastCall.args[0], {count: 10});
    });

    it("tees to two throughs and two sinks");
});
