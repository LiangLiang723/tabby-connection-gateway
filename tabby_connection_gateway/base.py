import json
import logging
import websockets

# Apply patch to handle "line too long" errors
def patch_websockets():
    """Patch websockets library to handle larger HTTP headers"""
    try:
        import websockets.legacy.http
        
        # Store original function
        if not hasattr(websockets.legacy.http, '_original_read_line'):
            websockets.legacy.http._original_read_line = websockets.legacy.http.read_line
            
            async def patched_read_line(stream):
                """Patched version of read_line with increased limit"""
                line = b""
                max_line_length = 64 * 1024  # 64KB instead of default 4KB
                
                while True:
                    try:
                        chunk = await stream.read(1)
                    except Exception:
                        if line:
                            return line
                        raise
                        
                    if not chunk:
                        if line:
                            return line
                        raise ConnectionResetError("connection lost")
                        
                    line += chunk
                    if line.endswith(b'\r\n'):
                        return line[:-2]  # Remove \r\n
                    if line.endswith(b'\n'):
                        return line[:-1]   # Remove \n
                        
                    if len(line) > max_line_length:
                        from websockets.exceptions import SecurityError
                        raise SecurityError("line too long")
            
            websockets.legacy.http.read_line = patched_read_line
            return True
    except Exception as e:
        # Silently fail if we can't patch
        return False
    return False

# Apply the patch immediately
_patch_applied = patch_websockets()


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
        self.log.info(f'Max message size: {self.max_message_size / 1024 / 1024:.1f} MB')
        
        # Log patch status
        if _patch_applied:
            self.log.info('WebSocket line length patch applied successfully')
        else:
            self.log.warning('WebSocket line length patch could not be applied')
            
        if self.ssl:
            self.log.info('Authorized CAs:')
            for cert in self.ssl.get_ca_certs():
                subject = dict(x[0] for x in cert['subject'])
                self.log.info(' - ' + (','.join('='.join(x) for x in subject.items())))

        async def process_request(path, request_headers):
            # Log incoming connection attempts
            self.log.debug(f'WebSocket connection attempt to path: {path}')
            self.log.debug(f'Request headers count: {len(request_headers)}')
            
            # Log header sizes to debug large headers
            total_header_size = 0
            for name, value in request_headers:
                header_size = len(name) + len(value) + 4  # +4 for ": " and "\r\n"
                total_header_size += header_size
                if header_size > 8192:  # Log any unusually large headers
                    self.log.warning(f'Large header detected: {name} ({header_size} bytes)')
            
            self.log.debug(f'Total headers size: {total_header_size} bytes')
            
            # Return None to accept all connections
            return None

        # Create custom server configuration for handling large headers
        server_config = {
            'host': self.host,
            'port': self.port,
            'ssl': self.ssl,
            'logger': logging.getLogger('websockets.server'),
            'process_request': process_request,
            'max_size': self.max_message_size,
            'read_limit': self.max_message_size,
            'write_limit': self.max_message_size,
        }
        
        # Try to add ping/pong configuration for better connection handling
        try:
            server_config.update({
                'ping_interval': 20,
                'ping_timeout': 20,
                'close_timeout': 10,
            })
        except TypeError:
            # Some versions might not support these parameters
            pass

        await websockets.serve(
            lambda w, path: self.handler(w),
            **server_config
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
