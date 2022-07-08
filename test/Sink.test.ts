import {Sink} from "../index";
import {assert} from "chai";

describe("Sink", function() {
    it("is a class", function() {
        assert.isFunction(Sink);
    });

    it("is not readable", function() {
        const s = new Sink();
        assert.isFalse(s.isReadable);
    });

    it("is writable", function() {
        const s = new Sink();
        assert.isTrue(s.isWritable);
    });
});
