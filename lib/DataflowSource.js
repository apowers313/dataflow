const {ReadableStream} = require("node:stream/web");
const DataflowChunk = require("./DataflowChunk");

async function timeout(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

module.exports = class DataflowSource {
    constructor(cfg = {}) {
        // if (typeof cfg.pull !== "function" && typeof this.pull !== "function") {
        //     throw new TypeError("expected to have a property named 'pull' that is of type 'function'");
        // }

        // this.pull = this.pull || cfg.pull;
        this.name = cfg.name || "<unknown>";
        // this.readableStream = new ReadableStream({
        //     // start: cfg.start,
        //     // pull: this.testPull,
        //     // cancel: cfg.cancel,
        //     pull: this.testPull.bind(this),
        // });
        this.readableStream = new ReadableStream({
            pull: async(controller) => {
                // TODO: remove timeout
                await timeout(500);
                let data = await this.pull(controller);
                if (!(data instanceof DataflowChunk)) {
                    data = new DataflowChunk({data});
                }

                console.log("data", data);
                controller.enqueue(data);
            },
        });
    }

    pipeTo(dst) {
        if (dst.writableStream) {
            dst = dst.writableStream;
        }

        console.log("piping to", dst);
        console.log("this.readableStream", this.readableStream);
        let ret = this.readableStream.pipeTo(dst);
        console.log("piped to", dst);
        console.log("this.readableStream", this.readableStream);
        console.log("ret", ret);
        return ret;
    }
};
