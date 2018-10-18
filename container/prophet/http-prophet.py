import logging

import time
import BaseHTTPServer
import pandas as pd
from SocketServer import ThreadingMixIn
from fbprophet import Prophet


HOST_NAME = '0.0.0.0'
PORT_NUMBER = 9090

logger = logging.getLogger(__name__)

class ProphetHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(s):
        """Respond to a GET request."""
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()
        s.wfile.write("OK")

    def do_POST(s):
        """Respond to a POST request."""
        content_length = int(s.headers.getheader('content-length', 0))
        request_json = s.rfile.read(content_length)

        df_raw = pd.read_json(request_json, orient='records')
        df = df_raw.rename(index=str, columns={"value": "y"})
        df['ds'] = pd.to_datetime(df['time'], unit='ms')

        m = Prophet()
        m.fit(df)
        future = m.make_future_dataframe(periods=0)
        forecast = m.predict(future)

        response_raw = forecast.rename(index=str, columns={"yhat": "value"})
        response_raw['logicalIndex'] = response_raw.index
        df = df.reset_index(drop=True)
        response_raw = response_raw.reset_index(drop=True)
        response_raw['time'] = df['time']
        response_json = response_raw[['time', 'value', 'logicalIndex']].to_json(orient='records')

        s.send_response(200)
        s.send_header("Content-type", "application/json")
        s.end_headers()
        s.wfile.write(response_json)

class ThreadedHTTPServer(ThreadingMixIn, BaseHTTPServer.HTTPServer):
    """Handle requests in a separate thread."""

if __name__ == '__main__':
    server_class = ThreadedHTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), ProphetHandler)
    logger.info("Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logger.info("Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))
