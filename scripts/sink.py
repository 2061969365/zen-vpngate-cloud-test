"""sink.py — 本地明文接收端：只记录请求，不碰上游。

为什么不用 TLS 终结型抓包代理对比 CLI 和网关：
    那条链路里代理由 Python/Go 的 TLS 栈去连 opencode.ai，ClientHello 指纹
    就变了，上游会回 429。于是"抓到 429"是抓包工具自己造成的。

要看的是**请求体/头本身的差异**，与 TLS 无关，所以两边都指到本地明文 HTTP
接收端：CLI 和网关各自把真实的上游请求原样吐出来，逐字段 diff。

用法：
    python sink.py --port 19100 --out bodies_cli
      然后让客户端 baseURL 指向 http://127.0.0.1:19100/inference/v1
"""

import argparse
import json
import os
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {"n": 0, "out": "bodies"}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, code, obj):
        payload = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self._json(200, {"data": [], "object": "list"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            STATE["n"] += 1
            seq = STATE["n"]
            os.makedirs(STATE["out"], exist_ok=True)
            slug = self.path.strip("/").replace("/", "_") or "root"
            path = os.path.join(STATE["out"], f"{seq:03d}-{slug}.json")

            record = {
                "seq": seq,
                "ts": time.time(),
                "method": "POST",
                "path": self.path,
                "headers": [[k, v] for k, v in self.headers.items()],
                "body_len": len(raw),
                "body": raw.decode("utf-8", "replace"),
            }
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, indent=1)
            print(f"#{seq} {self.path} {len(raw)}B -> {path}", flush=True)

            # 回一个最小可用的 Chat SSE，让客户端正常收尾（不发上游，不耗额度）
            sse = (
                b'data: {"id":"chatcmpl-sink","object":"chat.completion.chunk",'
                b'"created":0,"model":"sink","choices":[{"index":0,'
                b'"delta":{"role":"assistant","content":"ok"},'
                b'"finish_reason":null}]}\n\n'
                b'data: {"id":"chatcmpl-sink","object":"chat.completion.chunk",'
                b'"created":0,"model":"sink","choices":[{"index":0,"delta":{},'
                b'"finish_reason":"stop"}],'
                b'"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}\n\n'
                b'data: [DONE]\n\n'
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(sse)))
            self.end_headers()
            self.wfile.write(sse)
        except Exception:
            traceback.print_exc()
            try:
                self._json(500, {"error": "sink failure"})
            except Exception:
                pass

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=19100)
    parser.add_argument("--out", default="bodies")
    args = parser.parse_args()
    STATE["out"] = args.out
    os.makedirs(args.out, exist_ok=True)
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"sink on http://127.0.0.1:{args.port}  out={args.out}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
