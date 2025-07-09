import { WebSocketServer, WebSocket } from "ws";
import { DebugSession } from "vscode";
import { output } from "./main";

interface RemoteServerOptions {
  port?: number;
}

interface ExecuteLldbCommandRequest {
  type: "request";
  command: "executeLldbCommand";
  lldb_command: string;
  id: string;
}

interface ExecuteLldbCommandResponse {
  type: "response";
  command: "executeLldbCommand";
  success: boolean;
  result?: string;
  error?: string;
  id: string;
}

type Message = ExecuteLldbCommandRequest | ExecuteLldbCommandResponse;

export class RemoteServer {
  private wss: WebSocketServer | null = null;
  private clients: Set<WebSocket> = new Set();
  private debugSession: DebugSession | null = null;

  constructor() {}

  listen(options: boolean | RemoteServerOptions): Promise<number> {
    return new Promise((resolve, reject) => {
      const port =
        typeof options === "object" && options.port ? options.port : 0;

      this.wss = new WebSocketServer({ port }, () => {
        const actualPort = (this.wss.address() as any).port;
        output.appendLine(
          `Remote WebSocket server listening on port ${actualPort}`
        );
        resolve(actualPort);
      });

      this.wss.on("error", (err) => {
        output.appendLine(`Remote WebSocket server error: ${err.message}`);
        reject(err);
      });

      this.wss.on("connection", (ws: WebSocket) => {
        output.appendLine("Remote client connected");
        this.clients.add(ws);

        ws.on("message", async (data: Buffer) => {
          try {
            const message = JSON.parse(data.toString()) as Message;
            await this.handleMessage(ws, message);
          } catch (err) {
            output.appendLine(`Error handling message: ${err.message}`);
            this.sendError(ws, "unknown", `Invalid message: ${err.message}`);
          }
        });

        ws.on("close", () => {
          output.appendLine("Remote client disconnected");
          this.clients.delete(ws);
        });

        ws.on("error", (err) => {
          output.appendLine(`WebSocket error: ${err.message}`);
        });
      });
    });
  }

  close() {
    if (this.wss) {
      output.appendLine("Closing remote WebSocket server");

      // Close all client connections
      for (const client of this.clients) {
        client.close();
      }
      this.clients.clear();

      // Close the server
      this.wss.close();
      this.wss = null;
    }
  }

  setDebugSession(session: DebugSession | null) {
    this.debugSession = session;
  }

  private async handleMessage(ws: WebSocket, message: Message) {
    switch (message.type) {
      case "request":
        await this.handleRequest(ws, message as ExecuteLldbCommandRequest);
        break;
      default:
        this.sendError(
          ws,
          "unknown",
          `Unknown message type: ${(message as any).type}`
        );
    }
  }

  private async handleRequest(
    ws: WebSocket,
    request: ExecuteLldbCommandRequest
  ) {
    switch (request.command) {
      case "executeLldbCommand":
        await this.handleExecuteLldbCommand(ws, request);
        break;
      default:
        this.sendError(ws, request.id, `Unknown command: ${request.command}`);
    }
  }

  private async handleExecuteLldbCommand(
    ws: WebSocket,
    request: ExecuteLldbCommandRequest
  ) {
    if (!this.debugSession) {
      this.sendResponse(ws, {
        type: "response",
        command: "executeLldbCommand",
        success: false,
        error: "No active debug session",
        id: request.id,
      });
      return;
    }

    try {
      // Send the command to the debug adapter
      const response = await this.debugSession.customRequest("evaluate", {
        expression: request.lldb_command,
        context: "remote",
      });

      this.sendResponse(ws, {
        type: "response",
        command: "executeLldbCommand",
        success: true,
        result: response.result,
        id: request.id,
      });
    } catch (err) {
      this.sendResponse(ws, {
        type: "response",
        command: "executeLldbCommand",
        success: false,
        error: err.message || "Command execution failed",
        id: request.id,
      });
    }
  }

  private sendResponse(ws: WebSocket, response: ExecuteLldbCommandResponse) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(response));
    }
  }

  private sendError(ws: WebSocket, id: string, error: string) {
    this.sendResponse(ws, {
      type: "response",
      command: "executeLldbCommand",
      success: false,
      error,
      id,
    });
  }

  // Send events to all connected clients
  broadcastEvent(event: any) {
    const message = JSON.stringify(event);
    for (const client of this.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  }
}
