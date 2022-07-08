import {Component} from "./Component";
import {Writable} from "./Writable";

/**
 * The end of a pipeline
 */
export class Sink extends Writable(Component) {}
