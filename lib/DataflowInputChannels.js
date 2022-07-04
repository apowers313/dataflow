const DataflowChunk = require("./DataflowChunk");
const {promiseState, isReadable, getReadableStream, isWritable, getWritableStream} = require("./utils");

module.exports = class DataflowInputChannels {
    constructor(cfg = {}) {
        if (!Array.isArray(cfg.src) || cfg.src.length < 2) {
            throw new TypeError("expected src to be an Array with at least two members");
        }

        cfg.src.forEach((s) => {
            if (!isReadable(s)) {
                throw new TypeError("expected all sources to be a readable dataflow component");
            }
        });

        if (!isWritable(cfg.dst)) {
            throw new TypeError("expected dst to be a writable dataflow component");
        }

        this.mode = cfg.mode || "fifo";
        if (!["fifo", "zipper", "batch"].includes(this.mode)) {
            throw new TypeError("expected mode to be one of 'fifo', 'zipper', or 'batch'");
        }

        this.sources = cfg.src;
        this.dest = cfg.dst;
        this.destStream = getWritableStream(cfg.dst);
        this.sourceStreams = cfg.src.map((s) => getReadableStream(s));
    }

    async runPipe() {
        let activeReaders = this.sourceStreams.map((s) => s.getReader());
        let readerPromises = activeReaders.map((r) => r.read());
        let {mode} = this;
        let writer = this.destStream.getWriter();

        return waitForReaders().then(async function processStreams() {
            let results = await getResults();

            // forward-loop sending results to maintain order of messages
            let batch = [];
            for (let i = 0; i < results.length; i++) {
                let data = results[i];
                if (data === null || data.done !== false) {
                    continue;
                }

                if (!(data.value instanceof DataflowChunk)) {
                    throw new TypeError("expected data to be of type DataflowChunk");
                }

                if (mode === "batch") {
                    batch.push(data.value);
                } else {
                    await writer.write(data.value);
                }

                readerPromises[i] = activeReaders[i].read();
            }

            if (mode === "batch" && batch.length) {
                let data = batch.reduce((m, d, idx) => {
                    m[idx] = d.data;
                    return m;
                }, {});
                await writer.write(new DataflowChunk({data}));
            }

            // backward-loop removing dead streams; forward-removing would make indexes invalid
            for (let i = results.length - 1; i >= 0; i--) {
                let data = results[i];
                if (data !== null &&
                    (data.done === true || data.done === "error")) {
                    removeReader(i);
                }
            }

            if (activeReaders.length === 0) {
                return true;
            }

            return waitForReaders().then(processStreams);
        });

        function waitForReaders() {
            switch (mode) {
            case "fifo":
                return Promise.race(readerPromises);
            case "batch":
            case "zipper":
                return Promise.all(readerPromises);
            default:
                throw new TypeError(`unknown DataflowMultiInput mode: ${mode}`);
            }
        }

        async function getResults() {
            let promiseStates = await Promise.all(readerPromises.map((r) => promiseState(r)));
            let results = [];

            for (let i = 0; i < promiseStates.length; i++) {
                switch (promiseStates[i]) {
                case "pending":
                    results.push(null);
                    break;
                case "fulfilled":
                    results.push(await readerPromises[i]);
                    break;
                case "rejected":
                    results.push({done: "error"});
                    break;
                default:
                    throw new Error("unknown promise state");
                }
            }

            return results;
        }

        function removeReader(n) {
            readerPromises.splice(n, 1);
            activeReaders.splice(n, 1);
        }
    }
};
