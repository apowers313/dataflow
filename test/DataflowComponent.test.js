const {assert} = require("chai");
const {DataflowComponent} = require("../index.js");

describe("DataflowComponent", function() {
    it("is a class", function() {
        assert.isFunction(DataflowComponent);
    });

    describe("readableMixin", function() {
        it("adds pipe", function() {
            let o = {methods: {}};
            DataflowComponent.readableMixin(o);
            assert.isFunction(o.pipe);
        });

        it("adds send", function() {
            let o = {methods: {}};
            DataflowComponent.readableMixin(o);
            assert.isFunction(o.send);
        });

        describe("channelized", function() {
            it("doesn't add pipe");
            it("doesn't add send");
            it("adds sendToChannel");
        });
    });

    describe("config", function() {
        it("has default name", function() {
            const c = new DataflowComponent();
            assert.strictEqual(c.name, "<unknown>");
        });

        it("sets name", function() {
            const c = new DataflowComponent({name: "bob"});
            assert.strictEqual(c.name, "bob");
        });
    });

    describe("log", function() {
        it("error");
        it("warn");
        it("info");
        it("debug");
        it("trace");
    });

    it("has symbol");
    it("has isReadable");
    it("has isWritable");
});
