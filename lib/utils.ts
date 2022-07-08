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

    alreadyDone(c: Component): boolean {
        return this.history.has(c);
    }

    add(component: Component | Array<Component>) {
        if (!Array.isArray(component)) {
            component = [component];
        }

        component.forEach((c) => {
            if (!this.history.has(c)) {
                this.remaining.add(c);
            }
        });
    }

    get(): Component {
        const [ret] = this.remaining;
        this.remaining.delete(ret);
        return ret;
    }
}

export type WalkCallbackFn = (c: Component) => void;

export function walkStream(c: Component, cb: WalkCallbackFn, ctx?: WalkContext) {
    if (!ctx) {
        ctx = new WalkContext();
    }

    if (isReadable(c)) {
        ctx.add(c.dests);
    }

    if (isWritable(c)) {
        ctx.add(c.srcs);
    }

    const next = ctx.get();
    walkStream(next, cb, ctx);
}
