const DataflowChannelizedChunks = require("./DataflowChannelizedChunks");
const DataflowComponent = require("./DataflowComponent");
const {isReadable, getReadableStream, getWritableStream} = require("./utils");

class DataflowRoutedOutput {
    constructor(cfg = {}) {
        if (!isReadable(cfg.src)) {
            throw new TypeError("expected src to be readable");
        }

        this.source = cfg.src;
        this.sourceStream = getReadableStream(cfg.src);

        if (!cfg.numChannels) {
            throw new TypeError("exepected number routed channels to be defined in advance");
        }

        this.channels = [];
        for (let i = 0; i < cfg.numChannels; i++) {
            this.channels[i] = new OutputChannel({parent: this, chNum: i});
        }

        this.source.methods.sendToChannel = this.sendToChannel.bind(this);
        this.source.methods.sendChunksToChannel = this.sendChunksToChannel.bind(this);
    }

    async sendToChannel(chanNum, data) {
        let cc = new DataflowChannelizedChunks({size: this.channels.length});
        cc.add(chanNum, data);
        this.sendChunksToChannel(cc);
    }

    async sendChunksToChannel(chanChunks) {
        // convert array to channel chunks
        if (Array.isArray(chanChunks)) {
            let data = chanChunks;
            chanChunks = new DataflowChannelizedChunks({size: data.length});
            data.forEach((d, idx) => {
                if (d !== null && d !== undefined) {
                    chanChunks.add(idx, d);
                }
            });
        }

        if (!(chanChunks instanceof DataflowChannelizedChunks)) {
            throw new TypeError("expected chanChunks to be an instance of DataflowChannelizedChunks");
        }

        this.source.controller.enqueue(chanChunks);
    }

    async runPipe() {
        const reader = this.sourceStream.getReader();
        let writers = this.channels.map((c) => c?.writer);

        let writerPromises = writers.map(() => Promise.resolve());

        let ret = allWritersReady().then(async function doWrite() {
            let chunk = await reader.read();

            if (chunk.done) {
                return;
            }

            let data = chunk.value;
            if (!(data instanceof DataflowChannelizedChunks)) {
                throw new TypeError("expected data to be an instance of DataflowChannelizedChunks");
            }

            data.channels.forEach((d, idx) => {
                if (d !== null && d !== undefined) {
                    writerPromises[idx] = writers[idx].write(d);
                }
            });

            await allWritersReady().then(doWrite);
        });
        this.source.pendingPromises.push(ret);
        return ret;

        async function allWritersReady() {
            return Promise.all(writerPromises);
        }
    }
}

class OutputChannel {
    constructor(cfg) {
        if (!(cfg.parent instanceof DataflowRoutedOutput)) {
            throw new TypeError("expected parent to be an instance of DataflowRoutedOutput");
        }

        if (typeof cfg.chNum !== "number") {
            throw new TypeError("expected chNum to be a number");
        }

        this.parent = cfg.parent;
        this.chNum = cfg.chNum;
        this.dest = null;
        this.writer = null;
        this.writableStream = null;
        this.methods = {};
        DataflowComponent.readableMixin(this, {
            pipeMethod: _outputChannelPipe,
            linkMethod: _outputChannelLink,
            sendMethod: _outputChannelSend,
        });
    }
}

function _outputChannelPipe(dst) {
    this.dest = dst;
    this.destStream = getWritableStream(dst);
    this.writer = this.destStream.getWriter();
}

function _outputChannelLink(dst) {
    dst.source = this.parent;
    this.dest = dst;
}

async function _outputChannelSend(msg) {
    return this.parent.source.methods.sendToChannel(this.chNum, msg);
}

module.exports = DataflowRoutedOutput;
