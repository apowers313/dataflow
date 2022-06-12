module.exports = class DataflowChunk {
    constructor(cfg = {}) {
        if (cfg.error instanceof Error) {
            this.error = cfg.error;
            this.type = "error";
            return;
        }

        this.type = cfg.type || "data";
        this.data = cfg.data || {};
    }
};
