const {DataflowSource} = require("../../index.js");

function timeout(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

class TestSource extends DataflowSource {
    constructor(opt = {}) {
        super({
            name: "test-source",
        });

        this.countBy = opt.countBy || 1;
        this.delay = opt.delay || 0;
        this.sendNum = opt.sendNum || 10;
        if (opt.countBy === undefined) {
            this.count = 0;
        } else {
            this.count = opt.countBy;
        }
    }

    async pull(methods) {
        if (this.delay) {
            await timeout(this.delay);
        }

        if (this.count > (this.sendNum * this.countBy)) {
            return methods.finished();
        }

        let next = {count: this.count};
        this.count += this.countBy;
        return next;
    }
}

class TestRoute extends DataflowSource {
    constructor(cfg = {}) {
        super({
            name: "test-route",
            numOutputs: cfg.numOutputs ?? 3,
        });

        this.count = 0;
        this.outputChan = cfg.outputChan ?? "round-robin";
    }

    async pull(methods) {
        if (this.count > 10) {
            return methods.finished();
        }

        if (this.outputChan === "round-robin") {
            return this.roundRobinPull(methods);
        }

        return this.normalPull(methods);
    }

    async roundRobinPull(methods) {
        let chNum = this.count % this.numOutputs;

        const next = {count: this.count};
        this.count++;
        await this.output.channels[chNum].send(next);
    }

    async normalPull(methods) {
        if (this.count > 10) {
            methods.finished();
        }

        const next = {count: this.count};
        this.count++;
        await this.output.channels[this.outputChan].send(next);
    }
}

module.exports = {
    timeout,
    TestSource,
    TestRoute,
};
