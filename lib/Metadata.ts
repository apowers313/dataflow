/**
 * An abstract class for representing all types of metadata
 */
export abstract class MetadataType {
    abstract name: string;
    abstract value: unknown;
}

export type MetadataTypeConstructor = new () => MetadataType;

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
     * @param type - The type to find, based on the constructor for the type
     * @returns The first instance of the type that was added to the collection, or null if no instance was found.
     */
    get<T extends MetadataTypeConstructor>(type: T): InstanceType<T> | null {
        for (let i = 0; i < this.#collection.length; i++) {
            const item = this.#collection[i];
            if (item instanceof type) {
                return item as InstanceType<T>;
            }
        }

        return null;
    }

    /**
     * Returns true if the collection has at least one instance of matching metadata, false otherwise
     *
     * @param type - The type to find, based on the constructor for the type
     * @returns True if the collection contains the specified type, false otherwise
     */
    has<T extends MetadataTypeConstructor>(type: T): boolean {
        return !!this.get(type);
    }

    /**
     * The number of members in the collection
     */
    get size(): number {
        return this.#collection.length;
    }
}

/**
 * A metadata element indicating that the stream has successfully completed
 */
export class DataflowEnd extends MetadataType {
    name = "dataflow-end";
    value = "end";
}

/**
 * A metadata element indicating that the stream has successfully started
 */
export class DataflowStart extends MetadataType {
    name = "dataflow-start";
    value = "start";
}
