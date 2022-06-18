const {ReadableStream, WritableStream} = require("node:stream/web");
const DataflowComponent = require("./DataflowComponent");
const {walkStream} = require("./utils");

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

    async complete() {
        let promises = [];
        walkStream(this, function(df) {
            if (df.writableStream instanceof WritableStream) {
                if (!(df.pipePromise instanceof Promise)) {
                    console.log(df);
                    throw new Error("sink doesn't have active pipe");
                }

                promises.push(df.pipePromise);
            }
        });

        return Promise.all(promises);
    }
};
