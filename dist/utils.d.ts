export declare const withResolvers: <T>() => {
    promise: Promise<T>;
    resolve: (value: T | PromiseLike<T>) => void;
    reject: (reason?: unknown) => void;
};
export declare const unwrapArrayIfSingle: <T extends unknown[]>(x: T) => T["length"] extends 1 ? T[0] : T;
export declare const readFileToWebStandardFile: (files: {
    filepath: string;
    originalFilename: string;
    mimetype: string;
    lastModifiedDate: Date;
}[]) => Promise<File[]>;
