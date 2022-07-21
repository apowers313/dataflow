import {Through} from "../index";
import {assert} from "chai";

describe("Through", function() {
    it("is a class", function() {
        assert.isFunction(Through);
        const t = new Through({name: "foo", numChannels: 4});
        assert.instanceOf(t, Through);
        assert.strictEqual(t.name, "foo");
        assert.strictEqual(t.numChannels, 4);
    });

    it("is readable", function() {
        const t = new Through();
        assert.isTrue(t.isReadable);
    });

    it("is writable", function() {
        const t = new Through();
        assert.isTrue(t.isWritable);
    });
});
