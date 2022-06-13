const {ReadableStream} = require("node:stream/web");
const DataflowChunk = require("./DataflowChunk");

module.exports = class DataflowSource {
    constructor(cfg = {}) {
        if (typeof cfg.pull !== "function" && typeof this.pull !== "function") {
            throw new TypeError("expected to have a property named 'pull' that is of type 'function'");
        }

        this.pull = this.pull || cfg.pull;
        this.name = cfg.name || "<unknown>";
        this.readableStream = new ReadableStream({
            pull: async(controller) => {
                let data = await this.pull(controller);
                if (data === null) {
                    controller.close();
                    return;
                }

                if (!(data instanceof DataflowChunk)) {
                    data = new DataflowChunk({data});
                }

                controller.enqueue(data);
            },
        });
    }

    pipeTo(dst) {
        if (dst.writableStream) {
            dst = dst.writableStream;
        }

        return this.readableStream.pipeTo(dst);
    }
};
