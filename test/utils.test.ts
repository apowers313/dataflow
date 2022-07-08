import {Sink, Source, Through, utils} from "../index";
const {isReadable, isWritable} = utils;
import {assert} from "chai";

describe("utils", function() {
    describe("isReadable", function() {
        it("is function", function() {
            assert.isFunction(isReadable);
        });

        it("correctly identifies components", function() {
            assert.isFalse(isReadable(new Source()));
            assert.isTrue(isReadable(new Through()));
            assert.isTrue(isReadable(new Sink()));
        });
    });

    describe("isWritable", function() {
        it("is function", function() {
            assert.isFunction(isWritable);
        });

        it("correctly identifies components", function() {
            assert.isFalse(isWritable(new Source()));
            assert.isTrue(isWritable(new Through()));
            assert.isTrue(isWritable(new Sink()));
        });
    });
});
