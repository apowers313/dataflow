import type {Component} from "./Component";
import type {ReadableType} from "./Readable";
import type {WritableType} from "./Writable";

export function isReadable(c: Component): c is ReadableType {
    return c.isReadable;
}

export function isWritable(c: Component): c is WritableType {
    return c.isWritable;
}

// TODO
// isSource, isSink, isThrough, isDataflowComponent

class WalkContext {
    history: Set<Component> = new Set();
    remaining: Set<Component> = new Set();

    // alreadyDone(c: Component): boolean {
    //     return this.history.has(c);
    // }

    done(c: Component) {
        this.history.add(c);
    }

    add(component: Component | Array<Component>) {
        if (!Array.isArray(component)) {
            component = [component];
        }

        component.forEach((c) => {
            console.log("history.has", c.name, this.history.has(c));
            if (!this.history.has(c)) {
                console.log("adding", c.name);
                this.remaining.add(c);
            }
        });
    }

    get(): Component | null {
        const [ret] = this.remaining;

        if (!ret) {
            return null;
        }

        this.remaining.delete(ret);
        return ret;
    }
}

export type WalkCallbackFn = (c: Component) => void;

export function walkStream(c: Component, cb: WalkCallbackFn, ctx?: WalkContext) {
    console.log("WALKING:", c.name);
    if (!ctx) {
        ctx = new WalkContext();
    }

    if (isReadable(c)) {
        ctx.add(c.dests);
    }

    if (isWritable(c)) {
        ctx.add(c.srcs);
    }

    cb(c);
    ctx.done(c);

    const next = ctx.get();
    if (!next) {
        console.log("no next, done");
        return;
    }

    walkStream(next, cb, ctx);
}
