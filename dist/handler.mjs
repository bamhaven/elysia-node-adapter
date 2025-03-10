// src/handler.ts
import { Readable } from "stream";
import { isNotEmpty, StatusMap } from "elysia/utils";
import { Cookie, serializeCookie } from "elysia/cookies";
import { ElysiaCustomStatusResponse } from "elysia/error";
import { ElysiaFile } from "elysia/universal/file";
import { mergeResponseWithSetHeaders } from "elysia/adapter/web-standard/handler";
var handleFile = (response, set, res) => {
  const size = response.size;
  if (set) {
    let setHeaders;
    if (set.headers instanceof Headers) {
      setHeaders = {
        "accept-ranges": "bytes",
        "content-range": `bytes 0-${size - 1}/${size}`,
        "transfer-encoding": "chunked"
      };
      for (const [key, value] of set.headers.entries())
        if (key in set.headers) setHeaders[key] = value;
    } else if (isNotEmpty(set.headers)) {
      Object.assign(
        {
          "accept-ranges": "bytes",
          "content-range": `bytes 0-${size - 1}/${size}`,
          "transfer-encoding": "chunked"
        },
        set.headers
      );
      setHeaders = set.headers;
    }
  }
  if (res)
    return response.arrayBuffer().then((arrayBuffer) => {
      set.headers["content-type"] = response.type;
      set.headers["content-range"] = `bytes 0-${arrayBuffer.byteLength - 1}/${arrayBuffer.byteLength}`;
      delete set?.headers["content-length"];
      const nodeBuffer = Buffer.from(arrayBuffer);
      res.writeHead(set.status, set.headers);
      res.end(nodeBuffer);
      return [nodeBuffer, set];
    });
  return [response, set];
};
var handleElysiaFile = async (response, set, res) => {
  let headers;
  let status;
  const [length, value] = await Promise.all([response.length, response.value]);
  if (!set) {
    headers = {
      "accept-range": "bytes",
      "content-type": response.type,
      // BigInt is >= 9007 terabytes, likely not possible in a file
      "content-range": `bytes 0-${length - 1}/${length}`
    };
    if (res) res.writeHead(200, headers);
    status = 200;
  } else {
    Object.assign(set.headers, {
      "accept-range": "bytes",
      "content-type": response.type,
      // BigInt is >= 9007 terabytes, likely not possible in a file
      "content-range": `bytes 0-${length - 1}/${length}`
    });
    if (res) res.writeHead(set.status, set.headers);
    status = set.status;
    headers = set.headers;
  }
  if (res) {
    ;
    value.pipe(res);
  }
  return [
    response,
    {
      status,
      headers
    }
  ];
};
var handleStream = (generator, set, res) => {
  if (!set)
    set = {
      status: 200,
      headers: {
        "transfer-encoding": "chunked",
        "content-type": "text/event-stream;charset=utf8"
      }
    };
  else {
    set.headers["transfer-encoding"] = "chunked";
    set.headers["content-type"] = "text/event-stream;charset=utf8";
  }
  if (res) res.writeHead(set.status, set.headers);
  return [handleStreamResponse(generator, set, res), set];
};
var handleStreamResponse = (generator, set, res) => {
  const readable = new Readable({
    read() {
    }
  });
  if (res) readable.pipe(res);
  (async () => {
    let init = generator.next();
    if (init instanceof Promise) init = await init;
    if (init.done) {
      if (set) return mapResponse(init.value, set, res);
      return mapCompactResponse(init.value, res);
    }
    if (init.value !== void 0 && init.value !== null) {
      if (typeof init.value === "object")
        try {
          readable.push(Buffer.from(JSON.stringify(init.value)));
        } catch {
          readable.push(Buffer.from(init.value.toString()));
        }
      else readable.push(Buffer.from(init.value.toString()));
    }
    for await (const chunk of generator) {
      if (chunk === void 0 || chunk === null) continue;
      if (typeof chunk === "object")
        try {
          readable.push(Buffer.from(JSON.stringify(chunk)));
        } catch {
          readable.push(Buffer.from(chunk.toString()));
        }
      else readable.push(Buffer.from(chunk.toString()));
      await new Promise((resolve) => setTimeout(() => resolve(), 0));
    }
    readable.push(null);
  })();
  return readable;
};
async function* streamResponse(response) {
  const body = response.body;
  if (!body) return;
  const reader = body.getReader();
  const decoder = new TextDecoder();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      yield decoder.decode(value);
    }
  } finally {
    reader.releaseLock();
  }
}
var mapResponse = (response, set, res) => {
  if (isNotEmpty(set.headers) || set.status !== 200 || set.cookie) {
    if (typeof set.status === "string") set.status = StatusMap[set.status];
    if (set.cookie && isNotEmpty(set.cookie)) {
      const cookie = serializeCookie(set.cookie);
      if (cookie) set.headers["set-cookie"] = cookie;
    }
  }
  switch (response?.constructor?.name) {
    case "String":
      set.headers["content-type"] = "text/plain;charset=utf8";
      if (res) {
        set.headers["content-length"] = Buffer.byteLength(
          response,
          "utf8"
        );
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "Array":
    case "Object":
      response = JSON.stringify(response);
      set.headers["content-type"] = "application/json;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "ElysiaFile":
      return handleElysiaFile(
        response,
        set,
        res
      );
    case "File":
    case "Blob":
      set.headers["content-length"] = response.size;
      return handleFile(response, set, res);
    case "ElysiaCustomStatusResponse":
      set.status = response.code;
      return mapResponse(
        response.response,
        set,
        res
      );
    case "ReadableStream":
      if (!set.headers["content-type"]?.startsWith("text/event-stream"))
        set.headers["content-type"] = "text/event-stream;charset=utf8";
      if (res) {
        res.writeHead(set.status, set.headers);
        readableStreamToReadable(response).pipe(res);
      }
      return [response, set];
    case void 0:
      if (!response) {
        if (res) {
          set.headers["content-length"] = 0;
          set.headers["content-type"] = "text/plain;charset=utf8";
          res.writeHead(set.status, set.headers);
          res.end("");
        }
        return ["", set];
      }
      response = JSON.stringify(response);
      set.headers["content-type"] = "application/json;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "Response":
      response = mergeResponseWithSetHeaders(response, set);
      if (response.headers.get("transfer-encoding") === "chunked")
        return handleStream(
          streamResponse(response),
          set,
          res
        );
      return [
        responseToValue(response, res, set),
        set
      ];
    case "Error":
      return errorToResponse(response, set, res);
    case "Promise":
      return response.then(
        (x) => mapResponse(x, set, res)
      );
    case "Function":
      return mapResponse(response(), set, res);
    case "Number":
    case "Boolean":
      response = response.toString();
      set.headers["content-type"] = "text/plain;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "Cookie":
      if (response instanceof Cookie)
        return mapResponse(response.value, set, res);
      return mapResponse(response?.toString(), set, res);
    case "FormData":
      if (res)
        responseToValue(
          new Response(response),
          res,
          set
        );
      return [response, set];
    default:
      if (response instanceof Response) {
        response = mergeResponseWithSetHeaders(
          response,
          set
        );
        return [
          responseToValue(
            response,
            res,
            set
          ),
          set
        ];
      }
      if (response instanceof Promise)
        return response.then((x) => mapResponse(x, set, res));
      if (response instanceof Error)
        return errorToResponse(response, set, res);
      if (response instanceof ElysiaCustomStatusResponse) {
        set.status = response.code;
        return mapResponse(
          response.response,
          set,
          res
        );
      }
      if (response instanceof ElysiaFile)
        return handleElysiaFile(
          response,
          set,
          res
        );
      if (typeof response?.next === "function")
        return handleStream(response, set, res);
      if (typeof response?.then === "function")
        return response.then((x) => mapResponse(x, set, res));
      if (typeof response?.toResponse === "function")
        return mapResponse(response.toResponse(), set, res);
      if ("charCodeAt" in response) {
        const code = response.charCodeAt(0);
        if (code === 123 || code === 91) {
          if (!set.headers["Content-Type"])
            set.headers["content-type"] = "application/json;charset=utf8";
          response = JSON.stringify(response);
          set.headers["content-length"] = Buffer.byteLength(
            response,
            "utf8"
          );
          if (res) {
            res.writeHead(set.status, set.headers);
            res.end(response);
          }
          return [response, set];
        }
      }
      set.headers["content-type"] = "text/plain;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
  }
};
var mapEarlyResponse = (response, set, res) => {
  if (response === void 0 || response === null) return;
  if (isNotEmpty(set.headers) || set.status !== 200 || set.cookie) {
    if (typeof set.status === "string") set.status = StatusMap[set.status];
    if (set.cookie && isNotEmpty(set.cookie)) {
      const cookie = serializeCookie(set.cookie);
      if (cookie) set.headers["set-cookie"] = cookie;
    }
  }
  switch (response?.constructor?.name) {
    case "String":
      set.headers["content-type"] = "text/plain;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "Array":
    case "Object":
      response = JSON.stringify(response);
      set.headers["content-type"] = "application/json;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "ElysiaFile":
      return handleElysiaFile(
        response,
        set,
        res
      );
    case "File":
    case "Blob":
      return handleFile(response, set, res);
    case "ElysiaCustomStatusResponse":
      set.status = response.code;
      return mapEarlyResponse(
        response.response,
        set,
        res
      );
    case "ReadableStream":
      if (!set.headers["content-type"]?.startsWith("text/event-stream"))
        set.headers["content-type"] = "text/event-stream;charset=utf8";
      if (res) {
        res.writeHead(set.status, set.headers);
        readableStreamToReadable(response).pipe(res);
      }
      return [response, set];
    case void 0:
      if (!response) {
        set.headers["content-length"] = 0;
        if (res) {
          res.writeHead(set.status, set.headers);
          res.end(response);
        }
        return ["", set];
      }
      response = JSON.stringify(response);
      set.headers["content-type"] = "application/json;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      return [response, set];
    case "Response":
      response = mergeResponseWithSetHeaders(response, set);
      if (response.headers.get("transfer-encoding") === "chunked")
        return handleStream(
          streamResponse(response),
          set,
          res
        );
      return [
        responseToValue(response, res, set),
        set
      ];
    case "Error":
      return errorToResponse(response, set, res);
    case "Promise":
      return response.then(
        (x) => mapEarlyResponse(x, set, res)
      );
    case "Function":
      return mapEarlyResponse(response(), set, res);
    case "Number":
    case "Boolean":
      response = response.toString();
      set.headers["content-type"] = "text/plain;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
    case "Cookie":
      if (response instanceof Cookie)
        return mapEarlyResponse(response.value, set, res);
      return mapEarlyResponse(response?.toString(), set, res);
    case "FormData":
      if (res)
        responseToValue(
          new Response(response),
          res,
          set
        );
      return [response, set];
    default:
      if (response instanceof Response) {
        response = mergeResponseWithSetHeaders(
          response,
          set
        );
        return [
          responseToValue(
            response,
            res,
            set
          ),
          set
        ];
      }
      if (response instanceof Promise)
        return response.then(
          (x) => mapEarlyResponse(x, set, res)
        );
      if (response instanceof Error)
        return errorToResponse(response, set, res);
      if (response instanceof ElysiaCustomStatusResponse) {
        set.status = response.code;
        return mapEarlyResponse(
          response.response,
          set,
          res
        );
      }
      if (typeof response?.next === "function")
        return handleStream(response, set, res);
      if (typeof response?.then === "function")
        return response.then(
          (x) => mapEarlyResponse(x, set, res)
        );
      if (typeof response?.toResponse === "function")
        return mapEarlyResponse(
          response.toResponse(),
          set,
          res
        );
      if ("charCodeAt" in response) {
        const code = response.charCodeAt(0);
        if (code === 123 || code === 91) {
          response = JSON.stringify(response);
          if (!set.headers["Content-Type"])
            set.headers["content-type"] = "application/json;charset=utf8";
          set.headers["content-length"] = Buffer.byteLength(
            response,
            "utf8"
          );
          if (res) {
            res.writeHead(set.status, set.headers);
            res.end(response);
          }
          return [response, set];
        }
      }
      set.headers["content-type"] = "text/plain;charset=utf8";
      set.headers["content-length"] = Buffer.byteLength(
        response,
        "utf8"
      );
      if (res) {
        res.writeHead(set.status, set.headers);
        res.end(response);
      }
      return [response, set];
  }
};
var mapCompactResponse = (response, res) => {
  return mapResponse(
    response,
    {
      status: 200,
      headers: {}
    },
    res
  );
};
var errorToResponse = (error, set, res) => {
  const response = JSON.stringify({
    name: error?.name,
    message: error?.message,
    cause: error?.cause
  });
  let status = set?.status;
  if (!status) status = 500;
  if (set?.status === 200) status = 500;
  let headers = set?.headers;
  if (!headers)
    headers = {
      "content-length": response.length,
      "content-type": "application/json;charset=utf8"
    };
  else {
    headers["content-length"] = response.length;
    headers["content-type"] = "application/json;charset=utf8";
  }
  if (res) {
    res.writeHead(status, headers);
    res.end(response);
  }
  return [
    response,
    {
      status,
      headers
    }
  ];
};
var readableStreamToReadable = (webStream) => new Readable({
  async read() {
    const reader = webStream.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        this.push(Buffer.from(value));
      }
    } catch (error) {
      this.destroy(error);
    }
  }
});
var responseToValue = (r, res, set) => {
  responseHeaderToNodeHeader(r, set, res);
  if (res) res.statusCode = r.status;
  return r.arrayBuffer().then((buffer) => {
    set.headers["content-length"] = buffer.byteLength;
    if (res) res.end(Buffer.from(buffer));
    return buffer;
  }).catch((error) => errorToResponse(error, void 0, res));
};
var responseHeaderToNodeHeader = (response, set, res) => {
  if (set.status !== response.status) set.status = response.status;
  for (const x of response.headers.entries()) {
    set.headers[x[0]] = x[1];
    if (res) res.setHeader(x[0], x[1]);
  }
};
export {
  errorToResponse,
  handleStreamResponse,
  mapCompactResponse,
  mapEarlyResponse,
  mapResponse,
  readableStreamToReadable,
  responseToValue,
  streamResponse
};
