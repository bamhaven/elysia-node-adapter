// src/utils.ts
import fs from "fs";
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
        if (fs.openAsBlob)
          resolve(
            fs.openAsBlob(files[i].filepath).then(
              (blob) => new File([blob], files[i].originalFilename, {
                type: files[i].mimetype,
                lastModified: files[i].lastModifiedDate.getTime()
              })
            )
          );
        else {
          const buffer = Array();
          const stream = fs.createReadStream(files[i].filepath);
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
export {
  readFileToWebStandardFile,
  unwrapArrayIfSingle,
  withResolvers
};
