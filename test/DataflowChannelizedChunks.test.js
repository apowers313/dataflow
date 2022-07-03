const {assert} = require("chai");
const {DataflowChannelizedChunks} = require("../index.js");

describe("DataflowChannelizedChunks", function() {
    it("is a class", function() {
        assert.isFunction(DataflowChannelizedChunks);
    });

    describe("add", function() {
        it("adds a chunk");
        it("converts arg to DataflowChunk");
        it("throws if chunk has already been added on a channel");
    });

    describe("addChunk", function() {
        it("adds all chunks to correct channels");
        it("throws if array is too small");
        it("throws if array is too large");
        it("skips null data");
    });
});
