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

    async pull() {
        if (this.delay) {
            await timeout(this.delay);
        }

        if (this.count > (this.sendNum * this.countBy)) {
            return null;
        }

        let next = {count: this.count};
        this.count += this.countBy;
        return next;
    }
}

module.exports = {
    timeout,
    TestSource,
};
