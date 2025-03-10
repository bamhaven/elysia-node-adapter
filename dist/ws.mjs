// src/ws.ts
import {
  redirect,
  serializeCookie as serializeCookie2,
  ValidationError
} from "elysia";
import { getSchemaValidator, isNotEmpty as isNotEmpty3, randomId as randomId2 } from "elysia/utils";

// src/index.ts
import { Readable } from "stream";
import formidable from "formidable";
import { Elysia } from "elysia";
import {
  isNotEmpty as isNotEmpty2,
  isNumericString,
  mergeLifeCycle,
  randomId
} from "elysia/utils";

// src/handler.ts
import { isNotEmpty, StatusMap } from "elysia/utils";
import { Cookie, serializeCookie } from "elysia/cookies";
import { ElysiaCustomStatusResponse } from "elysia/error";
import { ElysiaFile } from "elysia/universal/file";
import { mergeResponseWithSetHeaders } from "elysia/adapter/web-standard/handler";

// src/index.ts
import { WebStandardAdapter } from "elysia/adapter/web-standard";
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
      if (req.readable) return _body = Readable.toWeb(req);
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

// src/ws.ts
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
      if (set.cookie && isNotEmpty3(set.cookie)) {
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
          return _id = randomId2();
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
export {
  attachWebSocket,
  nodeWebSocketToServerWebSocket,
  requestToContext
};
