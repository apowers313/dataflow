import {Chunk, DataChunk} from "../index";
import {assert} from "chai";

describe("Chunk", function() {
    it("is a class", function() {
        assert.isFunction(Chunk);
    });

    it("creates a data chunk by default", function() {
        const data = Chunk.create();
        assert.strictEqual(data.type, "data");
    });

    it("throws on invalid chunk type");

    it("creates error chunk on error");

    describe("clone", function() {
        it("duplicates a chunk", function() {
            const d1 = Chunk.create({type: "data", data: {foo: "bar"}});
            const d2 = (d1 as DataChunk).clone();

            assert.notEqual(d1, d2);
            assert.instanceOf(d1, Chunk);
            assert.deepEqual((d1 as DataChunk).data, (d2 as DataChunk).data);
        });

        it("duplicates really weird objects");
    });

    describe("is", function() {
        it("Error");
        it("Data");
        it("Metadata");
    });
});
