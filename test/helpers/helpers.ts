/* eslint-disable jsdoc/require-jsdoc */
import {Chunk, Source, SourceMethods} from "../../index";

export function timeout(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

export interface TestSourceOpts {
    countBy?: number;
    delay?: number;
    sendNum?: number;
}

export class TestSource extends Source {
    countBy: number;
    delay: number;
    sendNum: number;
    count: number;

    constructor(opt: TestSourceOpts = {}) {
        super({
            pull: (methods) => {
                // // TODO: get rid of this
                // const m: SourceMethods = {
                //     send: methods.send,
                //     sendMulti: methods.sendMulti,
                //     finished: async() => {
                //         this.controller!.close;
                //     },
                // };
                // return this.testPull.call(this, m);
                return this.testPull.call(this, methods);
            },
            name: "test-source",
        });

        this.countBy = opt.countBy ?? 1;
        this.delay = opt.delay ?? 0;
        this.sendNum = opt.sendNum ?? 10;
        if (opt.countBy === undefined) {
            this.count = 0;
        } else {
            this.count = opt.countBy;
        }
    }

    async testPull(this: TestSource, methods: SourceMethods): Promise<void> {
        if (this.delay) {
            await timeout(this.delay);
        }

        if (this.count > (this.sendNum * this.countBy)) {
            await methods.finished();
            return;
        }

        const next = Chunk.create({type: "data", data: {count: this.count}});
        this.count += this.countBy;
        console.log("sending", next);
        await methods.send(0, next);
    }
}

// class TestRoute extends DataflowSource {
//     constructor(cfg = {}) {
//         super({
//             name: "test-route",
//             numOutputs: cfg.numOutputs ?? 3,
//         });

//         this.count = 0;
//         this.outputChan = cfg.outputChan ?? "round-robin";
//     }

//     async pull(methods) {
//         if (this.count > 10) {
//             return methods.finished();
//         }

//         if (this.outputChan === "round-robin") {
//             return this.roundRobinPull(methods);
//         }

//         return this.normalPull(methods);
//     }

//     async roundRobinPull() {
//         const chNum = this.count % this.numOutputs;

//         const next = {count: this.count};
//         this.count++;
//         await this.output.channels[chNum].send(next);
//     }

//     async normalPull() {
//         const next = {count: this.count};
//         this.count++;
//         await this.output.channels[this.outputChan].send(next);
//     }
// }
