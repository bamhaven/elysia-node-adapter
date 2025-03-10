"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/utils.ts
var utils_exports = {};
__export(utils_exports, {
  readFileToWebStandardFile: () => readFileToWebStandardFile,
  unwrapArrayIfSingle: () => unwrapArrayIfSingle,
  withResolvers: () => withResolvers
});
module.exports = __toCommonJS(utils_exports);
var import_fs = __toESM(require("fs"));
var withResolvers = () => {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};
var unwrapArrayIfSingle = (x) => {
  if (!Array.isArray(x)) return x;
  if (x.length === 1) return x[0];
  return x;
};
var readFileToWebStandardFile = (files) => {
  const buffers = [];
  for (let i = 0; i < files.length; i++)
    buffers.push(
      new Promise((resolve, reject) => {
        if (import_fs.default.openAsBlob)
          resolve(
            import_fs.default.openAsBlob(files[i].filepath).then(
              (blob) => new File([blob], files[i].originalFilename, {
                type: files[i].mimetype,
                lastModified: files[i].lastModifiedDate.getTime()
              })
            )
          );
        else {
          const buffer = Array();
          const stream = import_fs.default.createReadStream(files[i].filepath);
          stream.on("data", (chunk) => buffer.push(chunk));
          stream.on(
            "end",
            () => resolve(
              new File(
                [new Blob([Buffer.concat(buffer)])],
                files[i].originalFilename,
                {
                  type: files[i].mimetype,
                  lastModified: files[i].lastModifiedDate.getTime()
                }
              )
            )
          );
          stream.on(
            "error",
            (err) => reject(`error converting stream - ${err}`)
          );
        }
      })
    );
  return Promise.all(buffers);
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  readFileToWebStandardFile,
  unwrapArrayIfSingle,
  withResolvers
});
