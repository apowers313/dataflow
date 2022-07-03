const {TransformStream} = require("node:stream/web");
const DataflowComponent = require("./DataflowComponent.js");
const DataflowChunk = require("./DataflowChunk");

module.exports = class DataflowThrough extends DataflowComponent {
    constructor(cfg = {}) {
        super(cfg);

        if (typeof cfg.through !== "function" && typeof this.through !== "function") {
            throw new TypeError("expected to have a property named 'through' that is of type 'function'");
        }

        this.through = this.through || cfg.through;
        this.transformStream = new TransformStream({
            start: (controller) => {
                this.controller = controller;
                if (typeof cfg.start === "function") {
                    cfg.start(controller);
                }
            },
            transform: async(chunk, controller) => {
                if (!(chunk instanceof DataflowChunk)) {
                    throw new TypeError("DataflowSink: expected write data to be instance of DataflowChunk");
                }

                if (chunk.type !== "data") {
                    return;
                }

                // TODO: error, metadata

                let data = await this.through(chunk.data, controller);

                if (data !== null) {
                    this.send(data);
                }
            },
            flush: cfg.flush,
        });
        this.readableStream = this.transformStream.readable;

        DataflowComponent.readableMixin(this);
        DataflowComponent.writableMixin(this);
    }
};
