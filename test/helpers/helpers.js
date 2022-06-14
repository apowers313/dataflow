const {DataflowSource} = require("../../index.js");

function timeout(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

class TestSource extends DataflowSource {
    constructor() {
        super({
            name: "test-source",
        });
        this.count = 0;
    }

    async pull() {
        // await timeout(100);

        if (this.count > 10) {
            return null;
        }

        return {count: this.count++};
    }
}

module.exports = {
    timeout,
    TestSource,
};
