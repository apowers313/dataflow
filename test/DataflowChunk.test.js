const {assert} = require("chai");
const {DataflowChunk} = require("../index.js");

describe("DataflowChunk", function() {
    it("is a class", function() {
        assert.isFunction(DataflowChunk);
    });

    it("creates a data chunk by default", function() {
        let data = new DataflowChunk();
        assert.strictEqual(data.type, "data");
    });

    it("returns existing chunk if data is a DataflowChunk", function() {
        const d1 = new DataflowChunk({data: "foo"});
        const d2 = new DataflowChunk({data: d1});
        assert.strictEqual(d1, d2);
    });

    it("throws on invalid chunk type");

    it("creates error chunk on error");

    describe("clone", function() {
        it("clones a chunk", function() {
            const d1 = new DataflowChunk({data: "foo"});
            const d2 = d1.clone();

            assert.notEqual(d1, d2);
            assert.instanceOf(d1, DataflowChunk);
            assert.deepEqual(d1.data, d2.data);
        });
    });
});
