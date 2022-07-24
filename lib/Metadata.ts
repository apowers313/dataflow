export abstract class MetadataType {
    abstract name: string;
    abstract value: unknown;
}

export type MetadataTypeConstructor = new () => MetadataType;
export class MetadataCollection {
    #collection: Array<MetadataType> = [];

    add(type: MetadataType): void {
        this.#collection.push(type);
    }

    get<T extends MetadataTypeConstructor>(type: T): InstanceType<T> | null {
        for (let i = 0; i < this.#collection.length; i++) {
            const item = this.#collection[i];
            if (item instanceof type) {
                return item as InstanceType<T>;
            }
        }

        return null;
    }

    has<T extends MetadataTypeConstructor>(type: T): boolean {
        return !!this.get(type);
    }

    get size(): number {
        return this.#collection.length;
    }
}

export class DataflowEnd extends MetadataType {
    name = "dataflow-end";
    value = "end";

    print(): void {
        console.log("the end.");
    }
}

export class DataflowStart extends MetadataType {
    name = "dataflow-start";
    value = "start";
}

// export abstract class MetadataType<Value> {
//     abstract readonly namespace: string;
//     abstract readonly type: string;
//     abstract value: Value;

//     // is<T extends MetadataType<Value>>(): this is T {
//     //     console.log("this.constructor", this.constructor);
//     //     console.log("this.constructor.name", this.constructor.name);
//     //     return true;
//     // }
//     is(ns: string, t: string): this is typeof this {
//         console.log("this.namespace", this.namespace, ns);
//         console.log("this.type", this.type, t);
//         return this.namespace === ns && this.type === t;
//     }

//     get(): Value {
//         return this.value;
//     }

//     set(v: Value): void {
//         this.value = v;
//     }
// }

// type TypeRegistry = Map<string, MetadataType<unknown>>;
// const metadataRegistry: Map<string, TypeRegistry> = new Map();

// export class Metadata {
//     static register(newType: MetadataType<unknown>): void {
//         let tr = metadataRegistry.get(newType.namespace);

//         if (!tr) {
//             tr = new Map();
//             metadataRegistry.set(newType.namespace, tr);
//         }

//         if (tr.has(newType.type)) {
//             throw new Error(`type already registered: ${newType.type}`);
//         }

//         tr.set(newType.type, newType);
//     }
// }

