import {Chunk} from "../index";
import {assert} from "chai";

describe("DataflowChunk", function() {
    it("is a class", function() {
        assert.isFunction(Chunk);
    });

    it("creates a data chunk by default", function() {
        const data = new Chunk();
        assert.strictEqual(data.type, "data");
    });

    it("returns existing chunk if data is a DataflowChunk", function() {
        const d1 = new Chunk({data: {foo: "bar"}});
        const d2 = new Chunk({data: d1});
        assert.strictEqual(d1, d2);
    });

    it("throws on invalid chunk type");

    it("creates error chunk on error");

    describe("clone", function() {
        it("duplicates a chunk", function() {
            const d1 = new Chunk({data: {foo: "bar"}});
            const d2 = d1.clone();

            assert.notEqual(d1, d2);
            assert.instanceOf(d1, Chunk);
            assert.deepEqual(d1.data, d2.data);
        });

        it("duplicates really weird objects");
    });
});
