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

    it("throws on invalid chunk type");

    it("creates error chunk on error");
});
