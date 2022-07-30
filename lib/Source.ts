import {Chunk, ChunkCollection, MetadataChunk} from "./Chunk";
import {Component, ComponentOpts} from "./Component";
import {ReadMethods, Readable, ReadableOpts} from "./Readable";
import {DataflowStart} from "./Metadata";
import {walkStream} from "./utils";

type FinishedFn = () => Promise<void>
export type SourcePullFn = (methods: SourceMethods) => Promise<void>

export interface SourceMethods extends ReadMethods {
    finished: FinishedFn;
}

export interface SourceOpts extends Omit<ReadableOpts, "pull"> {
    sendStartMetadata?: boolean;
    pull: SourcePullFn;
}

type SourceSuperOpts = ReadableOpts & ComponentOpts

/**
 * Generates data for a pipeline
 */
export class Source extends Readable(Component) {
    #sourcePull: SourcePullFn;
    sendStartMetadata: boolean;

    /**
     * Creates a source
     *
     * @param opts - Options for the new source
     */
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    constructor(opts: SourceOpts) {
        const inputOpts: SourceSuperOpts = {
            ... opts,
            pull: async(methods): Promise<void> => {
                await this.#sourcePull({
                    ... methods,
                    finished: async(): Promise<void> => {
                        this.readableController!.close();
                    },
                });
            },
            readStart: async(controller): Promise<void> => {
                if (opts.readStart) {
                    await opts.readStart(controller);
                }
            },
        };

        super(inputOpts);
        this.#sourcePull = opts.pull;
        this.sendStartMetadata = opts.sendStartMetadata ?? true;
    }

    /**
     * Waits for a dataflow to finish sending all data
     */
    async complete(): Promise<void> {
        const initPromises: Array<Promise<void>> = [];
        walkStream(this, (c) => {
            const p = c.init();
            initPromises.push(p);
        });

        await Promise.all(initPromises);
    }

    /**
     * Initializes the Source. Typically called by .complete()
     */
    async init(): Promise<void> {
        if (this.sendStartMetadata) {
            const mds = Chunk.create({type: "metadata"}) as MetadataChunk;
            mds.metadata.add(new DataflowStart());
            const cc = ChunkCollection.broadcast(mds, this.numChannels);
            this.readableController.enqueue(cc);
        }

        await super.init();
    }
}
