import type { IncomingMessage, ServerResponse } from 'http';
import { Readable } from 'stream';
import type { Context } from 'elysia/context';
import type { HTTPHeaders, Prettify } from 'elysia/types';
type SetResponse = Prettify<Omit<Context['set'], 'status'> & {
    status: number;
}>;
export type ElysiaNodeResponse = [
    response: unknown,
    set: Omit<Context['set'], 'headers' | 'status'> & {
        headers?: Omit<HTTPHeaders, 'content-length'> & {
            'content-length'?: number;
        };
        status: number;
    }
];
export declare const handleStreamResponse: (generator: Generator | AsyncGenerator, set?: Context["set"], res?: HttpResponse) => Readable;
export declare function streamResponse(response: Response): AsyncGenerator<string, void, unknown>;
type HttpResponse = ServerResponse<IncomingMessage> & {
    req: IncomingMessage;
};
export declare const mapResponse: (response: unknown, set: Context["set"], res?: HttpResponse) => ElysiaNodeResponse;
export declare const mapEarlyResponse: (response: unknown, set: Context["set"], res?: HttpResponse) => ElysiaNodeResponse | undefined;
export declare const mapCompactResponse: (response: unknown, res?: HttpResponse) => ElysiaNodeResponse;
export declare const errorToResponse: (error: Error, set?: Context["set"], res?: HttpResponse) => [string, {
    readonly status: number;
    readonly headers: any;
}];
export declare const readableStreamToReadable: (webStream: ReadableStream) => Readable;
export declare const responseToValue: (r: Response, res: HttpResponse | undefined, set: SetResponse) => Promise<ArrayBuffer | [string, {
    readonly status: number;
    readonly headers: any;
}]>;
export {};
