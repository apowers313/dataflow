const DataflowChunk = require("./DataflowChunk");
const {isReadable, getReadableStream, getWritableStream} = require("./utils");

module.exports = class DataflowMirroredOutput {
    constructor(cfg = {}) {
        if (!isReadable(cfg.src)) {
            throw new TypeError("expected src to be readable");
        }

        // TODO: error check cfg.dst

        this.source = cfg.src;
        this.dests = cfg.dst;
        this.sourceStream = getReadableStream(cfg.src);
        this.destStreams = this.dests.map((d) => getWritableStream(d));
        this.methods = {};
        const DataflowComponent = require("./DataflowComponent");
        DataflowComponent.readableMixin(this);
    }

    async runPipe() {
        const reader = this.sourceStream.getReader();
        const writers = this.destStreams.map((d) => d.getWriter());
        let writerPromises = writers.map(() => Promise.resolve());

        let ret = allWritersReady().then(async function doWrite() {
            let chunk = await reader.read();

            if (chunk.done) {
                return;
            }

            let data = chunk.value;
            if (!(data instanceof DataflowChunk)) {
                throw new TypeError("expected data to be an instance of DataflowChunk");
            }

            writerPromises = writers.map((w) => w.write(data.clone()));

            await allWritersReady().then(doWrite);
        });
        this.pendingPromises.push(ret);
        return ret;

        async function allWritersReady() {
            return Promise.all(writerPromises);
        }
    }
};
