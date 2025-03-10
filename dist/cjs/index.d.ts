import { type IncomingMessage } from 'http';
import formidable from 'formidable';
export declare const ElysiaNodeContext: unique symbol;
export declare const nodeRequestToWebstand: (req: IncomingMessage, abortController?: AbortController) => Request;
export declare const node: () => {
    name: string;
    handler: {
        mapResponse: (response: unknown, set: import("elysia").Context["set"], res?: import("http").ServerResponse<IncomingMessage> & {
            req: IncomingMessage;
        }) => import("./handler").ElysiaNodeResponse;
        mapEarlyResponse: (response: unknown, set: import("elysia").Context["set"], res?: import("http").ServerResponse<IncomingMessage> & {
            req: IncomingMessage;
        }) => import("./handler").ElysiaNodeResponse | undefined;
        mapCompactResponse: (response: unknown, res?: import("http").ServerResponse<IncomingMessage> & {
            req: IncomingMessage;
        }) => import("./handler").ElysiaNodeResponse;
    };
    composeHandler: {
        declare(inference: import("elysia/sucrose").Sucrose.Inference): string | undefined;
        mapResponseContext: string;
        headers: string;
        inject: {
            ElysiaNodeContext: symbol;
            nodeRequestToWebstand: (req: IncomingMessage, abortController?: AbortController) => Request;
            formidable: {
                (options?: formidable.Options): import("formidable/Formidable");
                defaultOptions: formidable.DefaultOptions;
                enabledPlugins: formidable.EnabledPlugins;
                plugins: formidable.EnabledPlugins;
                errors: {
                    FormidableError: {
                        new (message: string, internalCode: number, httpCode?: number): {
                            internalCode: number;
                            httpCode?: number | undefined;
                            name: string;
                            message: string;
                            stack?: string;
                            cause?: unknown;
                        };
                        captureStackTrace(targetObject: object, constructorOpt?: Function): void;
                        prepareStackTrace?: ((err: Error, stackTraces: NodeJS.CallSite[]) => any) | undefined;
                        stackTraceLimit: number;
                    };
                } & Record<"missingPlugin" | "pluginFunction" | "aborted" | "noParser" | "uninitializedParser" | "filenameNotString" | "maxFieldsSizeExceeded" | "maxFieldsExceeded" | "smallerThanMinFileSize" | "biggerThanMaxFileSize" | "noEmptyFiles" | "missingContentType" | "malformedMultipart" | "missingMultipartBoundary" | "unknownTransferEncoding", number>;
                File: typeof import("formidable/PersistentFile");
                PersistentFile: typeof import("formidable/PersistentFile");
                VolatileFile: typeof import("formidable/VolatileFile");
                Formidable: typeof import("formidable/Formidable");
                formidable: (options?: formidable.Options) => import("formidable/Formidable");
                IncomingForm: typeof import("formidable/Formidable");
                parsers: typeof import("formidable/parsers");
            } & formidable.MappedParsers;
            readFileToWebStandardFile: (files: {
                filepath: string;
                originalFilename: string;
                mimetype: string;
                lastModifiedDate: Date;
            }[]) => Promise<File[]>;
            unwrapArrayIfSingle: <T extends unknown[]>(x: T) => T["length"] extends 1 ? T[0] : T;
        };
        parser: {
            declare: string;
            json(): string;
            text(): string;
            urlencoded(): string;
            arrayBuffer(): string;
            formData(): string;
        };
    };
    composeGeneralHandler: {
        parameters: string;
        inject: {
            nodeRequestToWebstand: (req: IncomingMessage, abortController?: AbortController) => Request;
            ElysiaNodeContext: symbol;
        };
        createContext: (app: import("elysia").AnyElysia) => string;
        websocket(): string;
        error404(hasEventHook: boolean, hasErrorHook: boolean): {
            declare: string;
            code: string;
        };
    };
    composeError: {
        declare: string;
        inject: {
            ElysiaNodeContext: symbol;
        };
        mapResponseContext: string;
        validationError: string;
        unknownError: string;
    };
    ws(app: import("elysia").AnyElysia, path: string, options: any): void;
    listen(app: import("elysia").AnyElysia): (options: any, callback: import("elysia/universal/server").ListenCallback | undefined) => void;
};
export default node;
