import {Source} from "../index";
import {assert} from "chai";

describe("Source", function() {
    it("is a class", function() {
        assert.isFunction(Source);
        const s = new Source();
        assert.instanceOf(s, Source);
    });

    it("is readable", function() {
        const s = new Source();
        assert.isTrue(s.isReadable);
    });

    it("is not writable", function() {
        const s = new Source();
        assert.isFalse(s.isWritable);
    });
});
