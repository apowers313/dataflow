const {TransformStream} = require("node:stream/web");
const DataflowComponent = require("./DataflowComponent.js");
const DataflowChunk = require("./DataflowChunk");

module.exports = class DataflowThrough extends DataflowComponent {
    constructor(cfg = {}) {
        super(cfg);

        this.through = this.through || cfg.through;
        this.transformStream = new TransformStream({
            start: cfg.start,
            transform: async(chunk, controller) => {
                // TODO: check DataflowChunk, error, metadata, data
                let data = await this.through(chunk.data, controller);

                if (data !== null) {
                    this.send(controller, data);
                }
            },
            flush: cfg.flush,
        });
        this.readableStream = this.transformStream.readable;
    }
};