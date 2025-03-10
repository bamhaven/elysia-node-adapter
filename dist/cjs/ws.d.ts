import { AnyElysia, type Context, type TSchema } from 'elysia';
import type { TypeCheck } from 'elysia/type-system';
import { createServer, IncomingMessage, OutgoingMessage } from 'http';
import { WebSocketServer } from 'ws';
import type { WebSocket as NodeWebSocket } from 'ws';
import type { ServerWebSocket } from 'elysia/ws/bun';
export declare const nodeWebSocketToServerWebSocket: (ws: NodeWebSocket, wss: WebSocketServer, data: {
    id: string;
    validator?: TypeCheck<TSchema>;
}) => {
    send(data: string | import("elysia/ws/bun").BufferSource, compress: boolean | undefined): 0 | 1 | 2 | 3;
    sendText(data: string, compress: boolean | undefined): 0 | 1 | 2 | 3;
    sendBinary(data: import("elysia/ws/bun").BufferSource, compress: boolean | undefined): 0 | 1 | 2 | 3;
    close(code: number | undefined, reason: string | undefined): void;
    terminate(): void;
    ping(data: string | import("elysia/ws/bun").BufferSource | undefined): 0 | 1 | 2 | 3;
    pong(data: string | import("elysia/ws/bun").BufferSource | undefined): 0 | 1 | 2 | 3;
    publish(topic: string, data: string | import("elysia/ws/bun").BufferSource, _compress: boolean | undefined): 0 | 1 | 2 | 3;
    publishText(topic: string, data: string, _compress: boolean | undefined): 0 | 1 | 2 | 3;
    publishBinary(topic: string, data: import("elysia/ws/bun").BufferSource, _compress: boolean | undefined): 0 | 1 | 2 | 3;
    subscribe(topic: string): void;
    unsubscribe(topic: string): void;
    isSubscribed(topic: string): boolean;
    cork<T = unknown>(callback: (ws: ServerWebSocket<T>) => T): T;
    remoteAddress: string;
    readonly readyState: 0 | 1 | 2 | 3;
    readonly binaryType: any;
    data: {
        id: string;
        validator?: TypeCheck<TSchema>;
    };
};
export declare const requestToContext: (app: AnyElysia, request: IncomingMessage, response: OutgoingMessage) => Context;
export declare const attachWebSocket: (app: AnyElysia, server: ReturnType<typeof createServer>) => void;
