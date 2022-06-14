const {assert} = require("chai");
const {DataflowComponent} = require("../index.js");

describe("DataflowComponent", function() {
    it("is a class", function() {
        assert.isFunction(DataflowComponent);
    });

    it("has pipe method", function() {
        const c = new DataflowComponent();
        assert.isFunction(c.pipe);
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
});
