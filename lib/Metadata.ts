import {inspectSymbol} from "./utils";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Constructor<T = Record<any, any>> = new (... args: any[]) => T;
export type MetadataTypeConstructor = Constructor<MetadataType>;

export interface MetadataTypeOpts {
    namespace: string;
    name: string;
}

/**
 * An abstract class for representing all types of metadata
 */
export abstract class MetadataType {
    namespace: string;
    name: string;
    value: unknown;
    registered = false;

    constructor(opts: MetadataTypeOpts) {
        this.name = opts.name;
        this.namespace = opts.namespace;
    }

    is<T extends MetadataTypeConstructor>(type: T): type is T {
        return this instanceof type;
    }

    mustBe<T extends MetadataTypeConstructor>(type: T): T {
        if (!(this instanceof type)) {
            throw new TypeError(`expected type to be ${type}`);
        }

        return this as unknown as T;
    }
}

/**
 * A collection of metadata elements
 */
export class MetadataCollection {
    #collection: Array<MetadataType> = [];

    /**
     * Adds metadata to the collection
     *
     * @param md - The metadata to add to the collection
     */
    add(md: MetadataType): void {
        this.#collection.push(md);
    }

    // /**
    //  * Returns the first instance of the requested type, or null if it's not found
    //  *
    //  * @param type - The type to find, based on the constructor for the type
    //  * @returns The first instance of the type that was added to the collection, or null if no instance was found.
    //  */
    // get<T extends MetadataTypeConstructor>(type: T): InstanceType<T> | null {
    //     for (let i = 0; i < this.#collection.length; i++) {
    //         const item = this.#collection[i];
    //         if (item instanceof type) {
    //             return item as InstanceType<T>;
    //         }
    //     }

    //     return null;
    // }
    get(namespace: string, name: string): MetadataType | null {
        const type = MetadataRegistry.lookup(namespace, name);
        if (!type) {
            return null;
        }

        for (let i = 0; i < this.#collection.length; i++) {
            const item = this.#collection[i];
            if (item instanceof type) {
                return item;
            }
        }

        return null;
    }

    // /**
    //  * Returns true if the collection has at least one instance of matching metadata, false otherwise
    //  *
    //  * @param type - The type to find, based on the constructor for the type
    //  * @returns True if the collection contains the specified type, false otherwise
    //  */
    // has<T extends MetadataTypeConstructor>(type: T): boolean {
    //     return !!this.get(type);
    // }

    has(namespace: string, name: string): boolean {
        return !!this.get(namespace, name);
    }

    /**
     * The number of members in the collection
     */
    get size(): number {
        return this.#collection.length;
    }

    /**
     * Converts this object to a human-readable string
     *
     * @returns String representing the metadata
     */
    toString(): string {
        const metadataNames = this.#collection
            .map((mt) => MetadataRegistry.reverseLookup(mt.constructor))
            .map((desc) => desc.map((d) => `${d.namespace}::${d.name}`))
            .flat();

        return `MetadataCollection [${metadataNames.join(" ")}]`;
    }

    /**
     * Node.js's util.inpsect() looks for this symbol when converting objects to strings.
     *
     * @returns A string for node.js's util.inspect
     */
    [inspectSymbol](): string {
        return this.toString();
    }
}

export interface NamespaceDescriptor {
    namespace: string;
    name: string;
}

export class MetadataRegistry {
    static namespaces: Map<string, Map<string, MetadataTypeConstructor>> = new Map();

    static lookup(namespace: string, name: string): MetadataTypeConstructor | undefined {
        return MetadataRegistry.namespaces.get(namespace)?.get(name);
    }

    // TODO: Function?
    static reverseLookup(c: Function): Array<NamespaceDescriptor> {
        const nsMaps = [... MetadataRegistry.namespaces];
        console.log("nsMaps:", nsMaps);
        const namesMaps = nsMaps.map((m) => {
            const ns = m[0];
            const values = [... m[1]].map((v) => {
                v.unshift(ns);
                return v;
            });

            return values;
        }).flat() as unknown as Array<[string, string, MetadataTypeConstructor]>;

        return namesMaps.filter((v) => v[2] === c).map((v) => {
            return {namespace: v[0], name: v[1]};
        });
    }

    static register(namespace: string, name: string, type: MetadataTypeConstructor): void {
        let nsMap = MetadataRegistry.namespaces.get(namespace);

        if (!nsMap) {
            nsMap = new Map();
            MetadataRegistry.namespaces.set(namespace, nsMap);
        }

        if (nsMap.has(name)) {
            throw new Error(`'${name}' already registered in the MetadataRegistry`);
        }

        nsMap.set(name, type);
    }
}

// type DecoratorFn<T> = (c: T) => void;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export function registerMetadata<T extends MetadataTypeConstructor>(Base: T) {
    const RegisteredMetadata = class extends Base {
        // name!: string;
        // namespace!: string;
        // value!: unknown;

        constructor(... args: any[]) {
            super(... args);
            // eslint-disable-next-line @typescript-eslint/no-this-alias, @typescript-eslint/no-explicit-any
            // const regVal: any = this;
            MetadataRegistry.register(this.namespace, this.name, RegisteredMetadata);
            console.log(`Registered ${this.namespace}::${this.name}:`, RegisteredMetadata);
            console.log("Base", Base);
            console.log("RegisteredMetadata", RegisteredMetadata);
            this.registered = true;
        }

        get [Symbol.toStringTag](): string {
            return Base.name;
        }
    };

    return RegisteredMetadata;
}

/**
 * A metadata element indicating that the stream has successfully started
 */
@registerMetadata
export class DataflowStart extends MetadataType {
    constructor(sourceName: string) {
        super({namespace: "dataflow", name: "start"});
        this.value = sourceName;
    }
}

/**
 * A metadata element indicating that the stream has successfully completed
 */
@registerMetadata
export class DataflowEnd extends MetadataType {
    value = "end";

    constructor() {
        super({namespace: "dataflow", name: "end"});
    }
}
