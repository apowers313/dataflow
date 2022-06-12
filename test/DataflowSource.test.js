const {assert} = require("chai");
const {DataflowSource} = require("../index.js");
const {WritableStream} = require("node:stream/web");

class TestSource extends DataflowSource {
    constructor() {
        super({
            name: "test-source",
        });
        this.count = 0;
        console.log("TestSource.testPull", this.testPull);
    }

    async pull() {
        console.log("test pull", this.count);
        // await timeout(500);
        return {count: this.count++};
    }
}

function timeout(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

describe("DataflowSource", function() {
    it("is a class", function() {
        assert.isFunction(DataflowSource);
    });

    it("writes data", async function() {
        console.log("test source");
        let testSource = new TestSource();
        const source = testSource.readableStream;

        let testSink = new WritableStream({
            start: () => {
                console.log("SINK: start");
            },
            write: (data) => {
                console.log("SINK: data", data);
                // done();
            },
            close: () => {
                throw new Error("closed");
            },
            abort: () => {
                throw new Error("aborted");
            },
        });

        console.log("pipeTo");
        return source.pipeTo(testSink);
    });
});
