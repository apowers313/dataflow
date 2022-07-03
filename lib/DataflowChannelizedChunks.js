const DataflowChunk = require("./DataflowChunk");

module.exports = class DataflowChannelizedChunks {
    constructor(cfg = {}) {
        if (typeof cfg.size !== "number") {
            throw new TypeError("expected size to be a number");
        }

        this.size = cfg.size;
        this.channels = new Array(cfg.size).fill(null);
    }

    add(chNum, data) {
        if (!(data instanceof DataflowChunk)) {
            data = new DataflowChunk({data});
        }

        if (this.channels[chNum] !== null) {
            throw new Error(`attempting to overwrite data on channel ${chNum}`);
        }

        this.channels[chNum] = data;
    }

    addChunks(dataArray) {
        if (!Array.isArray(dataArray) || dataArray.length !== this.size) {
            throw new TypeError(`expected dataArray to be an array of length ${this.size}`);
        }

        dataArray.forEach((d, idx) => {
            if (d !== null && d !== undefined) {
                this.add(idx, d);
            }
        });
    }
};
