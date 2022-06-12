const WritableStream = require("node:stream/web");
const DataflowChunk = require("./DataflowChunk");

module.exports = class DataflowSink {
    constructor(cfg = {}) {
        if (typeof cfg.write !== "function") {
            throw new TypeError("expected 'write' parameter to be of type 'function'");
        }

        this.write = cfg.write;

        this.writeableStream = new WritableStream({
            start: cfg.start,
            write: this._write,
            close: cfg.close,
            abort: cfg.abort,
        });
    }

    async _write(chunk) {
        try {
            if (!(chunk instanceof DataflowChunk)) {
                throw new TypeError("write() expected 'chunk' to be of type 'DataflowChunk'");
            }

            if (chunk.type !== "data") {
                return;
            }

            await this.write(chunk.data, chunk);
        } catch (e) {
            chunk = new DataflowChunk({error: e, data: chunk});
            await this.write(chunk);
        }
    }
};
