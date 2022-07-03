module.exports = class DataflowChunk {
    constructor(cfg = {}) {
        if (cfg.data instanceof DataflowChunk) {
            return cfg.data;
        }

        if (cfg.error instanceof Error) {
            this.error = cfg.error;
            this.type = "error";
            return this;
        }

        this.type = cfg.type || "data";
        this.data = cfg.data || {};
    }

    clone() {
        return new DataflowChunk({
            data: structuredClone(this.data),
            type: this.type,
            error: this.error,
        });
    }
};
