import {Sink} from "../index";
import {assert} from "chai";

// eslint-disable-next-line @typescript-eslint/no-empty-function
async function push(): Promise<void> { }

describe("Sink", function() {
    it("is a class", function() {
        assert.isFunction(Sink);
    });

    it("is not readable", function() {
        const s = new Sink({push});
        assert.isFalse(s.isReadable);
    });

    it("is writable", function() {
        const s = new Sink({push});
        assert.isTrue(s.isWritable);
    });
});
