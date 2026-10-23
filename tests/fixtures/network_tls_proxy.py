#!/usr/bin/env python3
"""Disposable real TLS ingress for network tests; never logs bearer values."""

import argparse
import http.client
import http.server
import json
from pathlib import Path
import ssl
import time

parser = argparse.ArgumentParser()
parser.add_argument("--upstream-port", type=int, required=True)
parser.add_argument("--ready", type=Path, required=True)
parser.add_argument("--events", type=Path, required=True)
parser.add_argument("--mode", default="proxy")
args = parser.parse_args()
fixtures = Path(__file__).parent / "tls"


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *unused):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 4 * 1024 * 1024:
            self.send_error(413)
            return
        body = self.rfile.read(length)
        request = json.loads(body)
        with args.events.open("a") as stream:
            stream.write(json.dumps({"method": request.get("method"),
                                     "body_token": "token" in request.get("params", {}),
                                     "bearer_present": self.headers.get("Authorization", "").startswith("Bearer ")}) + "\n")
        if args.mode == "redirect":
            self.send_response(307)
            self.send_header("Location", "https://localhost:1/stolen")
            self.end_headers()
            return
        if args.mode == "slow":
            time.sleep(0.5)
        if args.mode in ("malformed", "oversize"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(4 * 1024 * 1024 + 1) if args.mode == "oversize" else "1")
            self.end_headers()
            if args.mode == "malformed":
                self.wfile.write(b"{")
            return
        connection = http.client.HTTPConnection("127.0.0.1", args.upstream_port, timeout=10)
        headers = {"Content-Type": "application/json", "Host": "localhost"}
        if self.headers.get("Authorization"):
            headers["Authorization"] = self.headers["Authorization"]
        for name in ("Accept", "MCP-Protocol-Version", "Origin"):
            if self.headers.get(name):
                headers[name] = self.headers[name]
        connection.request("POST", self.path, body, headers)
        response = connection.getresponse()
        payload = response.read()
        connection.close()
        if args.mode == "drop":
            self.close_connection = True
            return
        self.send_response(response.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def do_GET(self):
        connection = http.client.HTTPConnection("127.0.0.1", args.upstream_port, timeout=10)
        headers = {"Host": "localhost"}
        if self.headers.get("Authorization"):
            headers["Authorization"] = self.headers["Authorization"]
        connection.request("GET", self.path, headers=headers)
        response = connection.getresponse()
        payload = response.read()
        connection.close()
        self.send_response(response.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
server.daemon_threads = True
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.minimum_version = ssl.TLSVersion.TLSv1_2
context.load_cert_chain(fixtures / "server.pem", fixtures / "server-key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
ready_tmp = args.ready.with_suffix(".tmp")
ready_tmp.write_text(json.dumps({"port": server.server_address[1]}))
ready_tmp.replace(args.ready)
server.serve_forever()
