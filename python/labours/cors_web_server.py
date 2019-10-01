import threading


class CORSWebServer(object):
    def __init__(self):
        self.thread = threading.Thread(target=self.serve)
        self.server = None

    def serve(self):
        outer = self

        from http.server import HTTPServer, SimpleHTTPRequestHandler, test

        class ClojureServer(HTTPServer):
            def __init__(self, *args, **kwargs):
                HTTPServer.__init__(self, *args, **kwargs)
                outer.server = self

        class CORSRequestHandler(SimpleHTTPRequestHandler):
            def end_headers(self):
                self.send_header("Access-Control-Allow-Origin", "*")
                SimpleHTTPRequestHandler.end_headers(self)

        test(CORSRequestHandler, ClojureServer)

    def start(self):
        self.thread.start()

    def stop(self):
        if self.running:
            self.server.shutdown()
            self.thread.join()

    @property
    def running(self):
        return self.server is not None


web_server = CORSWebServer()
