import {Sink, Source, Through, utils} from "../index";
const {isReadable, isWritable, walkStream} = utils;
import {assert} from "chai";
import {spy} from "sinon";

describe("utils", function() {
    describe("isReadable", function() {
        it("is function", function() {
            assert.isFunction(isReadable);
        });

        it("correctly identifies components", function() {
            assert.isTrue(isReadable(new Source()));
            assert.isTrue(isReadable(new Through()));
            assert.isFalse(isReadable(new Sink()));
        });
    });

    describe("isWritable", function() {
        it("is function", function() {
            assert.isFunction(isWritable);
        });

        it("correctly identifies components", function() {
            assert.isFalse(isWritable(new Source()));
            assert.isTrue(isWritable(new Through()));
            assert.isTrue(isWritable(new Sink()));
        });
    });

    describe("walkStream", function() {
        it("walks simple stream from source", function() {
            const src = new Source({name: "source"});
            const thru = new Through({name: "through"});
            const sink = new Sink({name: "sink"});

            src.outputs[0].pipe(thru);
            thru.outputs[0].pipe(sink);

            const walkSpy = spy();
            walkStream(src, walkSpy);

            assert.deepEqual(walkSpy.args, [[src], [thru], [sink]]);
        });

        it("walks simple stream from middle", function() {
            const src = new Source({name: "source"});
            const thru = new Through({name: "through"});
            const sink = new Sink({name: "sink"});

            src.outputs[0].pipe(thru);
            thru.outputs[0].pipe(sink);

            const walkSpy = spy();
            walkStream(thru, walkSpy);

            // console.log("walkSpy.args", walkSpy.args);
            assert.deepEqual(walkSpy.args, [[thru], [sink], [src]]);
        });

        it("walks simple stream from sink", function() {
            const src = new Source({name: "source"});
            const thru = new Through({name: "through"});
            const sink = new Sink({name: "sink"});

            src.outputs[0].pipe(thru);
            thru.outputs[0].pipe(sink);

            const walkSpy = spy();
            walkStream(sink, walkSpy);

            console.log("walkSpy.args", walkSpy.args);
            assert.deepEqual(walkSpy.args, [[sink], [thru], [src]]);
        });
    });
});
