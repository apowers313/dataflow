import type {Component} from "./Component";
import type {ReadableType} from "./Readable";
import type {WritableType} from "./Writable";

/**
 * Indicates whether the specified componant can be read from (provides data).
 * Is also a TypeScript type guard for the ReadableType.
 *
 * @param c - A Component (Source, Sink, or Through)
 * @returns True if the Component is Readable, false otherwise
 */
export function isReadable(c: Component): c is ReadableType {
    return c.isReadable;
}

/**
 * Indicates whether the specified componant can be written to (consumes data).
 * Is also a TypeScript type guard for the WritableType.
 *
 * @param c - A Component (Source, Sink, or Through)
 * @returns True if the Component is Writable, false otherwise
 */
export function isWritable(c: Component): c is WritableType {
    return c.isWritable;
}

class WalkContext {
    history: Set<Component> = new Set();
    remaining: Set<Component> = new Set();

    // alreadyDone(c: Component): boolean {
    //     return this.history.has(c);
    // }

    done(c: Component): void {
        this.history.add(c);
    }

    add(component: Component | Array<Component>): void {
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

/**
 * Iterates all the components in the stream -- both upstream and downstream -- calling the specified calback for each.
 *
 * @param c - The starting component for iterating the stream. The first callback will be for this component.
 * @param cb - The callback that will be called for each component.
 * @param ctx - Used internally for tracking state throughout recursion.
 */
export function walkStream(c: Component, cb: WalkCallbackFn, ctx?: WalkContext): void {
    console.log("WALKING:", c.name);
    if (!ctx) {
        ctx = new WalkContext();
    }

    if (isReadable(c)) {
        console.log("isReadable");
        console.log("c.dests", c.dests);
        ctx.add(c.dests);
    }

    if (isWritable(c)) {
        console.log("isWritable");
        console.log("c.srcs", c.srcs);
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

type DeferredResolveFn<T> = (data: T) => void;
type DeferredRejectFn = (err: Error) => void;

/**
 * Contains a Promise that has not yet fulfilled, and the resolve / reject methodes for finalizing it.
 */
export class DeferredPromise<T> {
    /** The pending promise */
    promise: Promise<T>;
    /** The resolve function for the pending Promise */
    resolve!: DeferredResolveFn<T>;
    /** The reject function for the pending Promise */
    reject!: DeferredRejectFn;

    /**
     * Creates the new deferred Promise
     */
    constructor() {
        this.promise = new Promise<T>((res, rej) => {
            this.resolve = res;
            this.reject = rej;
        });
    }
}
