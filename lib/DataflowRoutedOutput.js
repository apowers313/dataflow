const DataflowChannelizedChunks = require("./DataflowChannelizedChunks");
const DataflowComponent = require("./DataflowComponent");
const {isReadable, getReadableStream, isWritable, getWritableStream} = require("./utils");

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

        console.log("num channels", cfg.numChannels);
        this.channels = [];
        for (let i = 0; i < cfg.numChannels; i++) {
            this.channels[i] = new OutputChannel({parent: this, chNum: i});
        }
        // this.channels = new Array(cfg.numChannels);
        // this.channels.map((c, idx) => {
        //     let ret = new OutputChannel({parent: this, chNum: idx});
        //     console.log("ret", ret);
        //     return ret;
        // });
        console.log("this.channels", this.channels);

        // DataflowComponent.readableMixin(this, {}, true);
        // this.methods = {};
        this.source.methods.sendToChannel = this.sendToChannel.bind(this);
        this.source.methods.sendChunksToChannel = this.sendChunksToChannel.bind(this);
    }

    async sendToChannel(chanNum, data) {
        console.log("doing sendToChannel");
        // if (!Array.isArray(data)) {
        //     data = [data];
        // }

        // if (!this.channels) {
        //     throw new Error("trying to send to channel when no channels exist");
        // }

        // data = new DataflowChunk(data);

        // let msg = {};
        // msg[chanNum] = data;
        // await sendMethod.call(this, msg);
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

        // if (!Array.isArray(dataArray)) {
        //     throw new TypeError("expected data to be an array");
        // }

        // if (!this.channels) {
        //     throw new Error("trying to send to channel when no channels exist");
        // }

        // if (this.warnChannelOverflow && (dataArray.length > (this.channels.length - 1))) {
        //     this.log.warn("Sending data to non-existant output channels");
        // }

        // if (this.warnChannelUnderflow && (dataArray.length < (this.channels.length - 1))) {
        //     this.log.warn("Not sending to all output channels");
        // }

        // dataArray = dataArray.map((d) => new DataflowChunk(d));

        // let msg = {};
        // dataArray.forEach((d, idx) => {
        //     if (d) {
        //         msg[idx] = d;
        //     }
        // });
        // await sendMethod.call(this, msg);
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
                console.log("data", data);
                throw new TypeError("expected data to be an instance of DataflowChannelizedChunks");
            }

            console.log("data", data);
            data.channels.forEach((d, idx) => {
                if (d !== null && d !== undefined) {
                    console.log("writing chunk", d);
                    writerPromises[idx] = writers[idx].write(d);
                }
            });
            // Object.keys(data).forEach((idx) => {
            //     console.log(`writers[${idx}]`, writers[idx]);
            //     if (!writers[idx].write) {
            //         throw new Error("writing to disconnected channel");
            //     }

            //     writerPromises[idx] = writers[idx].write(data);
            // });

            await allWritersReady().then(doWrite);
        });
        console.log("PUSHING PROMISE", ret);
        this.source.pendingPromises.push(ret);
        return ret;

        async function allWritersReady() {
            console.log("allWritersReady");
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

        console.log(`constructing channel ${cfg.chNum}`);

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
    console.log("doing output channel pipe");
    this.dest = dst;
    this.destStream = getWritableStream(dst);
    this.writer = this.destStream.getWriter();
}

function _outputChannelLink(dst) {
    dst.source = this.parent;
    this.dest = dst;
}

async function _outputChannelSend(msg) {
    // return this.writer?.write(msg);
    console.log("DOING SEND", msg);
    // TODO
    console.log("this.parent.source.methods.sendToChannel", this.parent.source.methods.sendToChannel);
    return this.parent.source.methods.sendToChannel(this.chNum, msg);
    // .then((res) => {
    //     console.log("send done:", res);
    // })
    // .catch((err) => {
    //     console.log("send failed:", err);
    // });
    // console.log("_outputChannelSend erroring");
    // throw new Error("foo!");
}

module.exports = DataflowRoutedOutput;
