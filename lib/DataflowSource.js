const {ReadableStream} = require("node:stream/web");
const DataflowComponent = require("./DataflowComponent");

module.exports = class DataflowSource extends DataflowComponent {
    constructor(cfg = {}) {
        super(cfg);

        if (typeof cfg.pull !== "function" && typeof this.pull !== "function") {
            throw new TypeError("expected to have a property named 'pull' that is of type 'function'");
        }

        this.pull = this.pull || cfg.pull;
        this.readableStream = new ReadableStream({
            pull: async(controller) => {
                let data = await this.pull(controller);
                if (data === null) {
                    controller.close();
                    return;
                }

                await this.send(controller, data);
            },
        });
    }
};
