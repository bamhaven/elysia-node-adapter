// src/index.ts
import { createServer } from "http";
import { Readable as Readable2 } from "stream";
import formidable from "formidable";
import { Elysia } from "elysia";
import {
  isNotEmpty as isNotEmpty3,
  isNumericString,
  mergeLifeCycle,
  randomId as randomId2
} from "elysia/utils";

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

// src/ws.ts
import {
  redirect,
  serializeCookie as serializeCookie2,
  ValidationError
} from "elysia";
import { getSchemaValidator, isNotEmpty as isNotEmpty2, randomId } from "elysia/utils";
import { WebSocketServer } from "ws";
import {
  createHandleWSResponse,
  createWSMessageParser,
  ElysiaWS
} from "elysia/ws";
import { parseSetCookies } from "elysia/adapter/web-standard/handler";
var nodeWebSocketToServerWebSocket = (ws, wss, data) => {
  const addListener = (message) => {
    for (const client of wss.clients)
      if (client !== ws && client.readyState === 1) client.send(message);
  };
  return {
    send(data2, compress) {
      ws.send(data2, {
        binary: Buffer.isBuffer(data2),
        compress
      });
      return ws.readyState;
    },
    sendText(data2, compress) {
      ws.send(data2, { binary: false, compress });
      return ws.readyState;
    },
    sendBinary(data2, compress) {
      ws.send(data2, { binary: true, compress });
      return ws.readyState;
    },
    close(code, reason) {
      ws.close(code, reason);
    },
    terminate() {
      ws.terminate();
    },
    ping(data2) {
      ws.ping(data2);
      return ws.readyState;
    },
    pong(data2) {
      ws.pong(data2);
      return ws.readyState;
    },
    publish(topic, data2, _compress) {
      ws.emit(topic, data2);
      return ws.readyState;
    },
    publishText(topic, data2, _compress) {
      ws.emit(topic, data2);
      return ws.readyState;
    },
    publishBinary(topic, data2, _compress) {
      ws.emit(topic, data2);
      return ws.readyState;
    },
    subscribe(topic) {
      ws.addListener(topic, addListener);
    },
    unsubscribe(topic) {
      ws.removeListener(topic, addListener);
    },
    isSubscribed(topic) {
      return ws.eventNames().includes(topic);
    },
    cork(callback) {
      return callback(this);
    },
    remoteAddress: "127.0.0.1",
    get readyState() {
      return ws.readyState;
    },
    get binaryType() {
      return ws.binaryType;
    },
    data
  };
};
var requestToContext = (app, request, response) => {
  const url = request.url;
  const s = url.indexOf("/", 11);
  const qi = url.indexOf("?", s + 1);
  const path = qi === -1 ? url.substring(s) : url.substring(s, qi);
  const set = {
    cookie: {},
    status: 200,
    headers: Object.assign(
      {},
      // @ts-expect-error private property
      app.setHeaders
    )
  };
  let _request;
  return Object.assign(
    {},
    // @ts-expect-error private property
    app.singleton.decorator,
    {
      // @ts-expect-error private property
      store: app.singleton.store,
      qi,
      path,
      url,
      set,
      redirect,
      get request() {
        if (_request) return _request;
        return _request = nodeRequestToWebstand(request);
      },
      [ElysiaNodeContext]: {
        req: request,
        res: void 0
      },
      headers: request.headers
    }
  );
};
var attachWebSocket = (app, server) => {
  const wsServer = new WebSocketServer({
    noServer: true
  });
  const staticWsRouter = app.router.static.ws;
  const router = app.router.http;
  const history = app.router.history;
  server.on("upgrade", (request, socket, head) => {
    wsServer.handleUpgrade(request, socket, head, async (ws) => {
      const qi = request.url.indexOf("?");
      let path = request.url;
      if (qi !== -1) path = request.url.substring(0, qi);
      const index = staticWsRouter[path];
      const route = index === void 0 ? history.find((item) => item.path === path && item.method === "$INTERNALWS") : history[index];
      if (!route) {
        request.destroy();
        return;
      }
      if (!route.handler) {
        request.destroy();
        return;
      }
      const websocket = route.handler;
      if (!websocket) {
        request.destroy();
        return;
      }
      const validateMessage = getSchemaValidator(route.hooks.body, {
        // @ts-expect-error private property
        modules: app.definitions.typebox,
        // @ts-expect-error private property
        models: app.definitions.type,
        normalize: app.config.normalize
      });
      const validateResponse = getSchemaValidator(
        route.hooks.response,
        {
          // @ts-expect-error private property
          modules: app.definitions.typebox,
          // @ts-expect-error private property
          models: app.definitions.type,
          normalize: app.config.normalize
        }
      );
      const parseMessage = createWSMessageParser(route.hooks.parse);
      const handleResponse = createHandleWSResponse(validateResponse);
      const context = requestToContext(app, request, void 0);
      const set = context.set;
      if (set.cookie && isNotEmpty2(set.cookie)) {
        const cookie = serializeCookie2(set.cookie);
        if (cookie) set.headers["set-cookie"] = cookie;
      }
      if (set.headers["set-cookie"] && Array.isArray(set.headers["set-cookie"]))
        set.headers = parseSetCookies(
          new Headers(set.headers),
          set.headers["set-cookie"]
        );
      if (route.hooks.upgrade) {
        if (typeof route.hooks.upgrade === "function") {
          const temp = route.hooks.upgrade(context);
          if (temp instanceof Promise) await temp;
          Object.assign(set.headers, context.set.headers);
        } else if (route.hooks.upgrade)
          Object.assign(
            set.headers,
            route.hooks.upgrade
          );
      }
      if (route.hooks.transform)
        for (let i = 0; i < route.hooks.transform.length; i++) {
          const hook = route.hooks.transform[i];
          const operation = hook.fn(context);
          if (hook.subType === "derive") {
            if (operation instanceof Promise)
              Object.assign(context, await operation);
            else Object.assign(context, operation);
          } else if (operation instanceof Promise) await operation;
        }
      if (route.hooks.beforeHandle)
        for (let i = 0; i < route.hooks.beforeHandle.length; i++) {
          const hook = route.hooks.beforeHandle[i];
          let response = hook.fn(context);
          if (hook.subType === "resolve") {
            if (response instanceof Promise)
              Object.assign(context, await response);
            else Object.assign(context, response);
            continue;
          } else if (response instanceof Promise)
            response = await response;
        }
      let _id;
      Object.assign(context, {
        get id() {
          if (_id) return _id;
          return _id = randomId();
        },
        validator: validateResponse
      });
      const elysiaWS = nodeWebSocketToServerWebSocket(
        ws,
        wsServer,
        context
      );
      if (websocket.open)
        handleResponse(
          elysiaWS,
          websocket.open(new ElysiaWS(elysiaWS, context))
        );
      if (websocket.message)
        ws.on("message", async (_message) => {
          const message = await parseMessage(elysiaWS, _message.toString());
          if (validateMessage?.Check(message) === false)
            return void ws.send(
              new ValidationError(
                "message",
                validateMessage,
                message
              ).message
            );
          handleResponse(
            elysiaWS,
            websocket.message(
              new ElysiaWS(elysiaWS, context, message),
              message
            )
          );
        });
      if (websocket.close)
        ws.on("close", (code, reason) => {
          handleResponse(
            elysiaWS,
            websocket.close(
              new ElysiaWS(elysiaWS, context),
              code,
              reason.toString()
            )
          );
        });
    });
  });
};

// src/index.ts
import { WebStandardAdapter } from "elysia/adapter/web-standard";

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

// src/index.ts
var ElysiaNodeContext = Symbol("ElysiaNodeContext");
var getUrl = (req) => {
  if (req.headers.host) return `http://${req.headers.host}${req.url}`;
  if (req.socket?.localPort)
    return `http://localhost:${req.socket?.localPort}${req.url}`;
  return `http://localhost${req.url}`;
};
var nodeRequestToWebstand = (req, abortController) => {
  let _signal;
  let _body;
  return new Request(getUrl(req), {
    method: req.method,
    headers: req.headers,
    get body() {
      if (req.method === "GET" || req.method === "HEAD") return null;
      if (_body !== void 0) return _body;
      if (req.readable) return _body = Readable2.toWeb(req);
      return null;
    },
    get signal() {
      if (_signal) return _signal;
      const controller = abortController ?? new AbortController();
      _signal = controller.signal;
      req.once("close", () => {
        controller.abort();
      });
      return _signal;
    },
    // @ts-expect-error
    duplex: "half"
  });
};
var node = () => {
  return {
    name: "node",
    handler: {
      mapResponse,
      mapEarlyResponse,
      mapCompactResponse
    },
    composeHandler: {
      declare(inference) {
        if (inference.request || inference.cookie)
          return `if(!('request' in c)){let _request
Object.defineProperty(c,'request',{get(){if(_request)return _request
return _request=nodeRequestToWebstand(c[ElysiaNodeContext].req)}})}
`;
      },
      mapResponseContext: "c[ElysiaNodeContext].res",
      headers: `c.headers=c[ElysiaNodeContext].req.headers
`,
      inject: {
        ElysiaNodeContext,
        nodeRequestToWebstand,
        formidable,
        readFileToWebStandardFile,
        unwrapArrayIfSingle
      },
      parser: {
        declare: `const req=c[ElysiaNodeContext].req
`,
        json() {
          let fnLiteral = `c.body=await new Promise((re)=>{let body
req.on('data',(chunk)=>{if(body) body=Buffer.concat([body,chunk])
else body=chunk})
req.on('end',()=>{`;
          fnLiteral += `if(!body || !body.length)return re()
else re(JSON.parse(body))`;
          return fnLiteral + `})})
`;
        },
        text() {
          let fnLiteral = `c.body=await new Promise((re)=>{let body
req.on('data',(chunk)=>{if(body) body=Buffer.concat([body,chunk])
else body=chunk})
req.on('end',()=>{`;
          fnLiteral += `if(!body || !body.length)return re()
else re(body)`;
          return fnLiteral + `})})
`;
        },
        urlencoded() {
          let fnLiteral = `c.body=await new Promise((re)=>{let body
req.on('data',(chunk)=>{if(body) body=Buffer.concat([body,chunk])
else body=chunk})
req.on('end',()=>{`;
          fnLiteral += `if(!body || !body.length)return re()
else re(parseQuery(body))`;
          return fnLiteral + `})})
`;
        },
        arrayBuffer() {
          let fnLiteral = `c.body=await new Promise((re)=>{let body
req.on('data',(chunk)=>{if(body) body=Buffer.concat([body,chunk])
else body=chunk})
req.on('end',()=>{`;
          fnLiteral += `if(!body || !body.length)return re()
else re(body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength))`;
          return fnLiteral + `})})
`;
        },
        formData() {
          return "const fields=await formidable({}).parse(req)\nc.body={}\nlet fieldKeys=Object.keys(fields[0])\nfor(let i=0;i<fieldKeys.length;i++){c.body[fieldKeys[i]]=unwrapArrayIfSingle(fields[0][fieldKeys[i]])}\nfieldKeys=Object.keys(fields[1])\nfor(let i=0;i<fieldKeys.length;i++){c.body[fieldKeys[i]]=unwrapArrayIfSingle(await readFileToWebStandardFile(fields[1][fieldKeys[i]]))}\n";
        }
      }
    },
    composeGeneralHandler: {
      parameters: "r,res",
      inject: {
        nodeRequestToWebstand,
        ElysiaNodeContext
      },
      createContext: (app) => {
        let decoratorsLiteral = "";
        let fnLiteral = `const qi=r.url.indexOf('?')
let p=r.url
if(qi!==-1)p=r.url.substring(0,qi)
`;
        const defaultHeaders = app.setHeaders;
        for (const key of Object.keys(app.singleton.decorator))
          decoratorsLiteral += `,${key}: decorator['${key}']`;
        const hasTrace = !!app.event.trace?.length;
        if (hasTrace) fnLiteral += `const id=randomId()
`;
        fnLiteral += `let _request
const c={`;
        if (app.inference.request || app.inference.cookie)
          fnLiteral += `get request(){if(_request)return _request
return _request = nodeRequestToWebstand(r)},`;
        fnLiteral += `store,qi,path:p,url:r.url,redirect,error,`;
        fnLiteral += "[ElysiaNodeContext]:{req:r,res},";
        fnLiteral += `set:{headers:`;
        fnLiteral += Object.keys(defaultHeaders ?? {}).length ? "Object.assign({}, app.setHeaders)" : "{}";
        fnLiteral += `,status:200}`;
        if (hasTrace) fnLiteral += ",[ELYSIA_REQUEST_ID]:id";
        fnLiteral += decoratorsLiteral;
        fnLiteral += `}
`;
        return fnLiteral;
      },
      websocket() {
        let fnLiteral = "";
        fnLiteral += `if (r.headers['upgrade'] === "websocket" && r.method === 'GET'){app.server.then((serv) => serv.raw.emit("upgrade", r, r.socket, Buffer.alloc(0)));return;}
`;
        return fnLiteral;
      },
      error404(hasEventHook, hasErrorHook) {
        let findDynamicRoute = `if(route===null){`;
        if (hasErrorHook)
          findDynamicRoute += `return app.handleError(c,notFound,false,${this.parameters})`;
        else
          findDynamicRoute += `if(c.set.status===200)c.set.status=404
res.writeHead(c.set.status, c.set.headers)
res.end(error404Message)
return [error404Message, c.set]`;
        findDynamicRoute += "}";
        return {
          declare: hasErrorHook ? "" : `const error404Message=notFound.message.toString()
`,
          code: findDynamicRoute
        };
      }
    },
    composeError: {
      declare: `
const res = context[ElysiaNodeContext].res
`,
      inject: {
        ElysiaNodeContext
      },
      mapResponseContext: ",res",
      validationError: `context.set.headers['content-type'] = 'application/json;charset=utf-8'
res.writeHead(context.set.status, context.set.headers)
res.end(error.message)
return [error.message, context.set]`,
      unknownError: `if(error.status)context.set.status=error.status
res.writeHead(context.set.status, context.set.headers)
res.end(error.message)
return [error.message, context.set]`
    },
    ws(app, path, options) {
      const key = Object.keys(app.router.history).length;
      app.router.static.ws[path] = key;
      const lifecycle = mergeLifeCycle(options, {});
      app.router.history.push({
        method: "$INTERNALWS",
        path,
        composed: void 0,
        handler: options,
        hooks: lifecycle
      });
      app.router.http.history.push(["$INTERNALWS", path, options]);
    },
    listen(app) {
      return (options, callback) => {
        app.compile();
        if (typeof options === "string") {
          if (!isNumericString(options))
            throw new Error("Port must be a numeric value");
          options = parseInt(options);
        }
        const webStandardApp = new Elysia({
          ...app.options,
          adapter: WebStandardAdapter
        }).use(app).compile();
        app.fetch = webStandardApp.fetch;
        const { promise: serverInfo, resolve: setServerInfo } = withResolvers();
        app.server = serverInfo;
        let server = createServer(
          // @ts-expect-error private property
          app._handle
        ).listen(
          typeof options === "number" ? options : {
            ...options,
            // @ts-ignore
            host: options?.hostname
          },
          () => {
            const address = server.address();
            const hostname = typeof address === "string" ? address : address ? address.address : "localhost";
            const port = typeof address === "string" ? 0 : address?.port ?? 0;
            const serverInfo2 = {
              id: randomId2(),
              development: process.env.NODE_ENV !== "production",
              fetch: app.fetch,
              hostname,
              // @ts-expect-error
              get pendingRequests() {
                const { promise, resolve, reject } = withResolvers();
                server.getConnections((error, total) => {
                  if (error) reject(error);
                  resolve(total);
                });
                return promise;
              },
              get pendingWebSockets() {
                return 0;
              },
              port,
              publish() {
                throw new Error(
                  "This adapter doesn't support uWebSocket Publish method"
                );
              },
              ref() {
                server.ref();
              },
              unref() {
                server.unref();
              },
              reload() {
                server.close(() => {
                  server = createServer(
                    // @ts-expect-error private property
                    app._handle
                  ).listen(
                    typeof options === "number" ? options : {
                      ...options,
                      // @ts-ignore
                      host: options?.hostname
                    }
                  );
                });
              },
              requestIP() {
                throw new Error(
                  "This adapter doesn't support Bun requestIP method"
                );
              },
              stop() {
                server.close();
              },
              upgrade() {
                throw new Error(
                  "This adapter doesn't support Web Standard Upgrade method"
                );
              },
              url: new URL(
                `http://${hostname === "::" ? "localhost" : hostname}:${port}`
              ),
              [Symbol.dispose]() {
                server.close();
              },
              // @ts-expect-error additional property
              raw: server
            };
            setServerInfo(serverInfo2);
            if (callback) callback(serverInfo2);
            app.modules.then(() => {
              try {
                serverInfo2.reload(
                  typeof options === "object" ? options : {
                    port: options
                  }
                );
              } catch {
              }
            });
          }
        );
        app.router.http.build?.();
        if (isNotEmpty3(app.router.static.ws) || app.router.http.root.ws || app.router.http.history.find((x) => x[0] === "ws"))
          attachWebSocket(app, server);
        if (app.event.start)
          for (let i = 0; i < app.event.start.length; i++)
            app.event.start[i].fn(this);
        process.on("beforeExit", () => {
          server.close();
          if (app.event.stop)
            for (let i = 0; i < app.event.stop.length; i++)
              app.event.stop[i].fn(this);
        });
      };
    }
  };
};
var index_default = node;
export {
  ElysiaNodeContext,
  index_default as default,
  node,
  nodeRequestToWebstand
};
