import asyncio
import hmac
import json
import logging
from typing import Set
import websockets

from .base import BaseServer, BaseWorker

log = logging.getLogger('server')


class GatewayWorker(BaseWorker):
    def __init__(self, server, websocket):
        self.server = server
        self.target_host = None
        self.target_port = None
        self._socket_reader = None
        self._websocket_reader = None
        self.reader = None
        self.writer = None
        super().__init__(websocket)
        self.log.info(f'New connection {self}')

    async def start(self):
        try:
            await self.send_service_message(
                {
                    '_': 'hello',
                    'version': 1,
                    'auth_required': not self.server.disable_auth,
                }
            )
            msg = await self.recv_service_message()
            if msg.get('_') != 'hello':
                return await self.fatal('expected-hello')

            if not self.server.disable_auth:
                if not msg.get('auth_token'):
                    return await self.fatal('expected-auth-token')

                valid_tokens = list(self.server.authorized_tokens)
                if self.server.permanent_auth_token:
                    valid_tokens.append(self.server.permanent_auth_token)

                for token in valid_tokens:
                    if hmac.compare_digest(msg['auth_token'], token):
                        if token in self.server.authorized_tokens:
                            self.server.authorized_tokens.remove(token)
                        break
                else:
                    return await self.fatal('incorrect-auth-token')

            await self.send_service_message({'_': 'ready'})

            msg = await self.recv_service_message()
            if msg.get('_') != 'connect':
                return await self.fatal('expected-connect')

            self.target_host = msg.get('host')
            self.target_port = msg.get('port')

            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.target_host, self.target_port
                )
            except Exception as e:
                self.log.info(f'Connection failed for {self}: {e}')
                return await self.fatal('connection-failed', details=str(e))

            self.log.info(f'Connection established for {self}')
            await self.send_service_message({'_': 'connected'})

            self._socket_reader = asyncio.get_event_loop().create_task(self.socket_reader())
            self._websocket_reader = asyncio.get_event_loop().create_task(
                self.websocket_reader()
            )
        except (json.JSONDecodeError, TypeError) as e:
            self.log.error(f'Invalid JSON message from {self}: {e}')
            return await self.fatal('invalid-message', details='Invalid JSON format')
        except websockets.exceptions.ConnectionClosed as e:
            self.log.info(f'Connection closed during handshake for {self}: {e}')
            return
        except Exception as e:
            self.log.error(f'Unexpected error during handshake for {self}: {e}')
            return await self.fatal('handshake-error', details=str(e))

    async def wait(self):
        if self._socket_reader:
            await self._socket_reader
        if self._websocket_reader:
            await self._websocket_reader

    async def close(self):
        await super().close()
        if not self.closed:
            try:
                if self.writer:
                    await self.writer.drain()
            except ConnectionResetError:
                pass
            if self.writer:
                self.writer.close()

    async def websocket_reader(self):
        while True:
            if self.closed:
                return
            try:
                data = await self.websocket.recv()
            except websockets.exceptions.ConnectionClosed:
                await self.close()
                return
            self.writer.write(data)

    async def socket_reader(self):
        while True:
            if self.closed:
                return
            try:
                await self.reader._wait_for_data('read')
            except TimeoutError:
                # TODO handle
                await self.close()
                return

            data = bytes(self.reader._buffer.copy())
            if not data:
                await self.close()
                return
            del self.reader._buffer[:]
            await self.websocket.send(data)


class GatewayServer(BaseServer):
    authorized_tokens: Set[str] = set()

    def __init__(self, *, host, port, ssl=None, auth_token=None, disable_auth=False, max_message_size=None):
        super().__init__(host, port, ssl, max_message_size=max_message_size)
        self.permanent_auth_token = auth_token
        self.disable_auth = disable_auth

    async def start(self):
        if self.permanent_auth_token:
            log.info('Token auth enabled')

        await super().start()

    async def handler(self, websocket):
        w = GatewayWorker(self, websocket)
        try:
            await w.start()
            await w.wait()
        except websockets.exceptions.ConnectionClosedError as e:
            w.log.info(f'Connection closed unexpectedly for {w}: {e}')
        except Exception as e:
            w.log.error(f'Handler error for {w}: {e}', exc_info=True)
        finally:
            await w.close()
