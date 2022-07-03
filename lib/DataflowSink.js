const {WritableStream} = require("node:stream/web");
const DataflowComponent = require("./DataflowComponent");
const DataflowChunk = require("./DataflowChunk");

module.exports = class DataflowSink extends DataflowComponent {
    constructor(cfg = {}) {
        super(cfg);

        if (typeof cfg.push !== "function" && typeof this.push !== "function") {
            throw new TypeError("expected 'push' method or parameter to be of type 'function'");
        }

        this.push = this.push || cfg.push;

        this.writableStream = new WritableStream({
            start: (controller) => {
                this.controller = controller;
                if (typeof cfg.start === "function") {
                    cfg.start(controller);
                }
            },
            write: async(chunk, controller) => {
                if (!(chunk instanceof DataflowChunk)) {
                    throw new TypeError("DataflowSink: expected write data to be instance of DataflowChunk");
                }

                console.log("got chunk", chunk);

                if (chunk.type !== "data") {
                    return;
                }

                await this.push(chunk.data, this.methods);
            },
            close: cfg.close,
            abort: cfg.abort,
        });

        DataflowComponent.writableMixin(this);
    }
};
