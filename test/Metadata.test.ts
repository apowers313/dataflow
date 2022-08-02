import {DataflowEnd, DataflowStart, MetadataCollection, MetadataRegistry, MetadataType} from "../index";
import {TestMetadata} from "./helpers/helpers";
import {assert} from "chai";
let bkupMap: Map<string, any>;

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
                const ret = mdc.get("dataflow", "end");
                assert.instanceOf(ret, DataflowEnd);
                assert.strictEqual(ret, dfe);
            });

            it("returns nothing", function() {
                const mdc = new MetadataCollection();
                const dfs = new DataflowStart("bob");
                mdc.add(dfs);
                const ret = mdc.get("dataflow", "end");
                assert.isNull(ret);
            });
        });
    });

    describe("MetadataType", function() {
        it("is function", function() {
            assert.isFunction(MetadataType);
        });
    });

    describe("MetadataRegistry", function() {
        before(function() {
            bkupMap = new Map(MetadataRegistry.namespaces);
            MetadataRegistry.namespaces.clear();
        });

        afterEach(function() {
            MetadataRegistry.namespaces.clear();
        });

        after(function() {
            MetadataRegistry.namespaces = new Map(bkupMap);
        });

        it("registers a type", function() {
            assert.strictEqual(MetadataRegistry.namespaces.size, 0);
            MetadataRegistry.register("test", "foo", TestMetadata);
            assert.strictEqual(MetadataRegistry.namespaces.size, 1);
            assert.isTrue(MetadataRegistry.namespaces.has("test"));
            assert.isTrue(MetadataRegistry.namespaces.get("test")?.has("foo"));
            assert.strictEqual(MetadataRegistry.namespaces.get("test")?.get("foo"), TestMetadata);
        });

        it("throws on re-register", function() {
            assert.strictEqual(MetadataRegistry.namespaces.size, 0);
            MetadataRegistry.register("test", "foo", TestMetadata);
            assert.throws(function() {
                MetadataRegistry.register("test", "foo", TestMetadata);
            }, Error, "'foo' already registered in the MetadataRegistry");
        });

        it("lookup", function() {
            MetadataRegistry.namespaces = new Map(bkupMap);
            MetadataRegistry.register("test", "foo", TestMetadata);

            const t = MetadataRegistry.lookup("test", "foo");
            assert.strictEqual(t, TestMetadata);
        });

        it("reverseLookup", function() {
            MetadataRegistry.namespaces = new Map(bkupMap);
            MetadataRegistry.register("test", "foo", TestMetadata);

            const desc = MetadataRegistry.reverseLookup(TestMetadata);
            assert.strictEqual(desc.length, 1);
            assert.strictEqual(desc[0].namespace, "test");
            assert.strictEqual(desc[0].name, "foo");
        });

        it("has DataflowStart", function() {
            new DataflowStart("bob");
            assert.isTrue(MetadataRegistry.has("dataflow", "start"));

            const desc = MetadataRegistry.reverseLookup(DataflowStart);
            assert.strictEqual(desc.length, 1);
            assert.strictEqual(desc[0].namespace, "dataflow");
            assert.strictEqual(desc[0].name, "start");
        });

        it("has DataflowEnd");
    });
});
