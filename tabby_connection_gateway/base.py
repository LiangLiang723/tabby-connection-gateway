import json
import logging
import websockets


class BaseServer:
    name = ''

    def __init__(self, host, port, ssl=None, max_message_size=None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.host = host
        self.port = port
        self.ssl = ssl
        self.max_message_size = max_message_size or (10 * 1024 * 1024)  # 10MB default

    async def start(self):
        self.log.info(f'Listening on {self.host}:{self.port}')
        if self.ssl:
            self.log.info('Authorized CAs:')
            for cert in self.ssl.get_ca_certs():
                subject = dict(x[0] for x in cert['subject'])
                self.log.info(' - ' + (','.join('='.join(x) for x in subject.items())))

        async def process_request(path, request_headers):
            # Log incoming connection attempts
            self.log.debug(f'WebSocket connection attempt to path: {path}')
            self.log.debug(f'Request headers: {dict(request_headers)}')
            # Return None to accept all connections
            return None

        await websockets.serve(
            lambda w, path: self.handler(w),
            self.host,
            self.port,
            ssl=self.ssl,
            # Add more detailed logging
            logger=logging.getLogger('websockets.server'),
            process_request=process_request,
            # Increase limits to handle large messages/headers
            max_size=self.max_message_size,
            read_limit=self.max_message_size,
        )


class BaseWorker:
    def __init__(self, websocket):
        self.websocket = websocket
        self.closed = False
        self.log = logging.getLogger(f'{self.__class__.__name__}[{self}]')

    def __str__(self):
        return f'{self.websocket.remote_address[0]}:{self.websocket.remote_address[1]}'

    async def start(self):
        raise NotImplementedError

    async def recv_service_message(self):
        return json.loads(await self.websocket.recv())

    async def send_service_message(self, data):
        await self.websocket.send(json.dumps(data))

    async def fatal(self, code, **kwargs):
        await self.send_service_message(
            {
                '_': 'error',
                'code': code,
                **kwargs,
            }
        )
        await self.close()

    async def wait(self):
        pass

    async def close(self):
        if not self.closed:
            self.closed = True
            self.log.info(f'Closing connection {self}')
