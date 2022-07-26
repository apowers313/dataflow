/* eslint-disable jsdoc/require-jsdoc */
import {Chunk, Source, SourceMethods} from "../../index";

// eslint-disable-next-line @typescript-eslint/no-empty-function
export async function through(): Promise<void> { }
// eslint-disable-next-line @typescript-eslint/no-empty-function
export async function push(): Promise<void> { }
// eslint-disable-next-line @typescript-eslint/no-empty-function
export async function pull(): Promise<void> { }

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

export interface TestRouteOpts {
    numChannels?: number;
    outputChan?: number;
    outputType?: TestRouteTypes;
}

export type TestRouteTypes = "round-robin" | "single-chan" | "broadcast";

export class TestRoute extends Source {
    count: number;
    outputChan: number;
    outputType: TestRouteTypes;

    constructor(cfg: TestRouteOpts = {}) {
        super({
            name: "test-route",
            numChannels: cfg.numChannels ?? 3,
            pull: async(methods: SourceMethods) => {
                await this.testPull.call(this, methods);
            },
        });

        this.count = 0;
        this.outputChan = cfg.outputChan ?? 0;
        this.outputType = cfg.outputType ?? "single-chan";
    }

    async testPull(this: TestRoute, methods: SourceMethods): Promise<void> {
        if (this.count > 10) {
            return methods.finished();
        }

        if (this.outputType === "round-robin") {
            return this.roundRobinPull(methods);
        }

        if (this.outputType === "broadcast") {
            console.log("BROADCAST PULL");
            return this.broadcastPull(methods);
        }

        return this.normalPull(methods);
    }

    async roundRobinPull(this: TestRoute, methods: SourceMethods): Promise<void> {
        const chNum = this.count % this.numChannels;

        const next = {count: this.count};
        this.count++;
        await methods.send(chNum, Chunk.create({type: "data", data: next}));
    }

    async normalPull(this: TestRoute, methods: SourceMethods): Promise<void> {
        const next = {count: this.count};
        this.count++;
        await methods.send(this.outputChan, Chunk.create({type: "data", data: next}));
    }

    async broadcastPull(this: TestRoute, methods: SourceMethods): Promise<void> {
        for (let i = 0; i < this.numChannels; i++) {
            const next = {count: `${i}-${this.count}`};
            console.log("XXX SENDING", i, next);
            await methods.send(i, Chunk.create({type: "data", data: next}));
        }
        this.count++;
    }
}
