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

    /**
     * Creates a new MetadataType
     *
     * @param opts - Options for the metadata type
     */
    constructor(opts: MetadataTypeOpts) {
        this.name = opts.name;
        this.namespace = opts.namespace;

        if (!MetadataRegistry.has(this.namespace, this.name)) {
            MetadataRegistry.register(this.namespace, this.name, this.constructor as MetadataTypeConstructor);
        }
    }

    /**
     * Type Guard for specific sub-types
     *
     * @param type - The type to validate
     * @returns - True if this object is the specified type. Also casts the type to the specified type.
     */
    is<T extends MetadataTypeConstructor>(type: T): type is T {
        return this instanceof type;
    }

    /**
     * Asserts the sub-type of the object, Acts as a Type Guard.
     *
     * @param type - The type to validate
     * @returns - The specified type
     */
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

    /**
     * Returns the first instance of the requested type, or null if it's not found
     *
     * @param namespace - The namespace to get the type from
     * @param name - The name of the type to get
     * @returns The first instance of the type that was added to the collection, or null if no instance was found.
     */
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

    /**
     * Returns true if the collection has at least one instance of matching metadata, false otherwise
     *
     * @param namespace - The namespace of the type to look up
     * @param name - THe name of the type to look up
     * @returns True if the collection contains the specified type, false otherwise
     */
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
            .map((mt) => MetadataRegistry.reverseLookup(mt.constructor as MetadataTypeConstructor))
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

/**
 * A registry singleton for metadata types
 */
export class MetadataRegistry {
    static namespaces: Map<string, Map<string, MetadataTypeConstructor>> = new Map();

    /**
     * Finds and returns the specified namespace / name pair
     *
     * @param namespace - The namespace to lookup
     * @param name - The name of the type to look up
     * @returns The constructor for the class of the specified type
     */
    static lookup(namespace: string, name: string): MetadataTypeConstructor | undefined {
        return MetadataRegistry.namespaces.get(namespace)?.get(name);
    }

    /**
     * Returns true of the specified namespace / name pair is defined in the registry
     *
     * @param namespace  - The namespace to lookup
     * @param name - The name to look up
     * @returns True if the specified namespace / name pair is found
     */
    static has(namespace: string, name: string): boolean {
        return !!this.lookup(namespace, name);
    }

    /**
     * Uses a class constructor to lookup all matching namespace / name pairs
     *
     * @param c - The constructor to use for the lookup
     * @returns An array of matching namespace / name pairs that are associated with the constructor
     */
    static reverseLookup(c: MetadataTypeConstructor): Array<NamespaceDescriptor> {
        const nsMaps = [... MetadataRegistry.namespaces];
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

    /**
     * Registers a new type at the specified namespace / name pair
     *
     * @param namespace - The namespace to use for registration. Will be created if it doesn't already exist.
     * @param name - The name of the type to use for registration
     * @param type - The MetadataType constructor to register
     */
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

/**
 * A metadata element indicating that the stream has successfully started
 */
export class DataflowStart extends MetadataType {
    /**
     * Creates a new DataflowStart metadata object
     *
     * @param sourceName - The name of the Source that has started emitting Chunks
     */
    constructor(sourceName: string) {
        super({namespace: "dataflow", name: "start"});
        this.value = sourceName;
    }
}

/**
 * A metadata element indicating that the stream has successfully completed
 */
export class DataflowEnd extends MetadataType {
    value = "end";

    /**
     * Creates a new DataflowEnd metadata object
     */
    constructor() {
        super({namespace: "dataflow", name: "end"});
    }
}
