const {ReadableStream} = require("node:stream/web");
const DataflowComponent = require("./DataflowComponent");
const DataflowRoutedOutput = require("./DataflowRoutedOutput");
const {walkStream, isRoute, isMirror} = require("./utils");

module.exports = class DataflowSource extends DataflowComponent {
    constructor(cfg = {}) {
        super(cfg);
        this.methods.finished = this.finished.bind(this);

        if (typeof cfg.pull !== "function" && typeof this.pull !== "function") {
            throw new TypeError("expected to have a property named 'pull' that is of type 'function'");
        }

        this.numOutputs = cfg.numOutputs || 1;
        this.pull = this.pull || cfg.pull;

        let streamCfg = {
            start: (controller) => {
                this.controller = controller;
                if (typeof cfg.start === "function") {
                    cfg.start(controller);
                }
            },
            pull: async() => {
                return this.pull(this.methods);
            },
            cancel: cfg.cancel,
        };

        this.readableStream = new ReadableStream(streamCfg);
        if (this.numOutputs > 1) {
            this.output = new DataflowRoutedOutput({src: this, numChannels: this.numOutputs});
        } else {
            DataflowComponent.readableMixin(this);
        }
    }

    async complete() {
        let promises = [];
        walkStream(this, function(df) {
            if (df.pendingPromises.length) {
                // console.log(`+++ adding pending promise for ${df.name}:`, df.pendingPromises);
                promises.push(df.pendingPromises);
            }

            if (isRoute(df)) {
                let p = df.output.runPipe();
                // console.log("+++ adding route promise");
                promises.push(p);
            }

            if (isMirror(df)) {
                let p = df.mirroredDest.runPipe();
                // console.log(`+++ adding mirrored promise for ${df.name}`);
                promises.push(p);
            }

            // if (isMultiInput(df)) {
            //     let p = df.runPipe();
            //     promises.push(p);
            // }
        });

        return Promise.all(promises.flat());
    }

    finished() {
        this.controller.close();
    }
};
