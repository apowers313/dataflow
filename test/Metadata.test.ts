import {DataflowEnd, DataflowStart, MetadataCollection, MetadataType} from "../index";
import {assert} from "chai";

describe("Metadata", function() {
    describe("MetadataCollection", function() {
        it("is function", function() {
            assert.isFunction(MetadataCollection);
        });

        describe("add", function() {
            it("adds a type", function() {
                const mdc = new MetadataCollection();
                assert.strictEqual(mdc.size, 0);
                mdc.add(new DataflowEnd());
                assert.strictEqual(mdc.size, 1);
            });
        });

        describe("get", function() {
            it("returns a type", function() {
                const mdc = new MetadataCollection();
                const dfe = new DataflowEnd();
                mdc.add(dfe);
                const ret = mdc.get(DataflowEnd);
                assert.instanceOf(ret, DataflowEnd);
                assert.strictEqual(ret, dfe);
            });

            it("returns nothing", function() {
                const mdc = new MetadataCollection();
                const dfs = new DataflowStart();
                mdc.add(dfs);
                const ret = mdc.get(DataflowEnd);
                assert.isNull(ret);
            });
        });
    });

    describe("MetadataType", function() {
        it("is function", function() {
            assert.isFunction(MetadataType);
        });
    });
});
