#!/usr/bin/env python
"""
swiftnbd. server module

Changed to support backy2 blocks instead of swift in 2015 by
Daniel Kraft <daniel.kraft@d9t.de>

Updated in 2018 by
Lars Fenneberg <lf@lemental.net>

Copyright (C) 2013-2015 by Juan J. Martinez <jjm@usebox.net>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import asyncio
import logging
import math
import signal
import struct
import traceback
from asyncio import StreamReader, StreamWriter
from typing import Generator, Optional, Any, Tuple

from benji.benji import BenjiStore
from benji.database import VersionUid, Version
from benji.repr import ReprMixIn


class _NbdServerAbortedNegotiationError(IOError):
    pass


class NbdServer(ReprMixIn):

    # Magics
    INIT_PASSWD = 0x4e42444d41474943  # b'NBDMAGIC'
    CLISERV_MAGIC = 0x49484156454F5054  # b'IHAVEOPT'
    NBD_OPT_REPLY_MAGIC = 0x3e889045565a9

    NBD_REQUEST_MAGIC = 0x25609513
    NBD_SIMPLE_REPLY_MAGIC = 0x67446698

    # Options (sent by the client)
    NBD_OPT_EXPORT_NAME = 1
    NBD_OPT_ABORT = 2
    NBD_OPT_LIST = 3
    NBD_OPT_PEEK_EXPORT = 4  # Not in use anymore
    NBD_OPT_STARTTLS = 5  # Not implemented
    NBD_OPT_INFO = 6
    NBD_OPT_GO = 7
    NBD_OPT_STRUCTURED_REPLY = 8  # Not implemented
    NBD_OPT_LIST_META_CONTEXT = 9  # Not implemented
    NBD_OPT_SET_META_CONTEXT = 10  # Not implemented
    NBD_OPT_EXTENDED_HEADERS = 11  # Not implemented

    NBD_OPT_MAP = {
        NBD_OPT_EXPORT_NAME: "export-name",
        NBD_OPT_ABORT: "abort",
        NBD_OPT_LIST: "list",
        NBD_OPT_PEEK_EXPORT: "peek-flush",
        NBD_OPT_STARTTLS: "starttls",
        NBD_OPT_INFO: "info",
        NBD_OPT_GO: "go",
        NBD_OPT_STRUCTURED_REPLY: "structured-reply",
        NBD_OPT_LIST_META_CONTEXT: "list-meta-context",
        NBD_OPT_SET_META_CONTEXT: "set-meta-context",
        NBD_OPT_EXTENDED_HEADERS: "extended-headers",
    }

    # Replies (sent by the server)
    NBD_REP_ACK = 1
    NBD_REP_SERVER = 2
    NBD_REP_INFO = 3
    NBD_REP_FLAG_ERROR = 1 << 31
    NBD_REP_ERR_UNSUP = NBD_REP_FLAG_ERROR | 1
    NBD_REP_ERR_POLICY = NBD_REP_FLAG_ERROR | 2  # Not used
    NBD_REP_ERR_INVALID = NBD_REP_FLAG_ERROR | 3
    NBD_REP_ERR_PLATFORM = NBD_REP_FLAG_ERROR | 4  # Not used
    NBD_REP_ERR_TLS_REQD = NBD_REP_FLAG_ERROR | 5  # Not used
    NBD_REP_ERR_UNKNOWN = NBD_REP_FLAG_ERROR | 6  # Not used
    NBD_REP_ERR_BLOCK_SIZE_REQD = NBD_REP_FLAG_ERROR | 8  # Not used

    # Command and command flags
    NBD_CMD_MASK_COMMAND = 0x0000ffff
    NBD_CMD_MASK_FLAGS = 0xffff0000
    NBD_CMD_FLAGS_SHIFT = 16

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3
    NBD_CMD_TRIM = 4  # Not implemented
    NBD_CMD_CACHE = 5  # Not implemented
    NBD_CMD_WRITE_ZEROES = 6  # Not implemented
    NBD_CMD_BLOCK_STATUS = 7  # Not implemented
    NBD_CMD_RESIZE = 8  # Not implemented (experimental resize extension)

    NBD_CMD_MAP = {
        NBD_CMD_READ: "read",
        NBD_CMD_WRITE: "write",
        NBD_CMD_DISC: "disconnect",
        NBD_CMD_FLUSH: "flush",
        NBD_CMD_TRIM: "trim",
        NBD_CMD_CACHE: "cache",
        NBD_CMD_WRITE_ZEROES: "write-zeroes",
        NBD_CMD_BLOCK_STATUS: "block-status",
        NBD_CMD_RESIZE: "resize",
    }

    NBD_CMD_FLAG_FUA = (1 << 0) << NBD_CMD_FLAGS_SHIFT  # Not implemented
    NBD_CMD_FLAG_NO_HOLE = (1 << 1) << NBD_CMD_FLAGS_SHIFT  # Not implemented (only relevant to NBD_CMD_WRITE_ZEROES)
    NBD_CMD_FLAG_DF = (1 << 2) << NBD_CMD_FLAGS_SHIFT  # Not implemented
    NBD_CMD_FLAG_REQ_ONE = (1 << 3) << NBD_CMD_FLAGS_SHIFT  # Not implemented

    # Handshake flags
    NBD_FLAG_FIXED_NEWSTYLE = 1 << 0
    NBD_FLAG_NO_ZEROES = 1 << 1

    # Contrary to the NBD specification which states:
    #   bit 1, NBD_FLAG_NO_ZEROES; if set, and if the client replies with NBD_FLAG_C_NO_ZEROES in the client flags
    #   field, the server MUST NOT send the 124 bytes of zero at the end of the negotiation.
    # at least nbd-client 3.19 will assume NBD_FLAG_NO_ZEROES even when the server doesn't advertise it.
    # This has been fixed in nbd-client via https://github.com/NetworkBlockDevice/nbd/commit/d5b2a76775803ea7d6378a8e9caa58d756b30940.
    NBD_HANDSHAKE_FLAGS = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES

    # Export flags
    NBD_FLAG_HAS_FLAGS = (1 << 0)
    NBD_FLAG_READ_ONLY = (1 << 1)
    NBD_FLAG_SEND_FLUSH = (1 << 2)
    NBD_FLAG_SEND_FUA = (1 << 3)
    NBD_FLAG_ROTATIONAL = (1 << 4)
    NBD_FLAG_SEND_TRIM = (1 << 5)
    NBD_FLAG_SEND_WRITE_ZEROES = (1 << 6)
    NBD_FLAG_CAN_MULTI_CONN = (1 << 8)

    # Out export flags: has flags, supports flush
    NBD_EXPORT_FLAGS = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH

    # command flags (upper 16 bit of request type)
    NBD_CMD_FLAG_FUA = (1 << 16)

    # Info types (not implemented)
    NBD_INFO_EXPORT = 0
    NBD_INFO_NAME = 1
    NBD_INFO_DESCRIPTION = 2
    NBD_INFO_BLOCK_SIZE = 3

    # Allowed errnos (as defined by the NBD protocol specification)
    EPERM = 1  # Operation not permitted.
    EIO = 5  # Input/output error.
    ENOMEM = 12  # Cannot allocate memory.
    EINVAL = 22  # Invalid argument.
    ENOSPC = 28  # No space left on device.
    EOVERFLOW = 75  # Value too large.
    ESHUTDOWN = 108  # Server is in the process of being shut down.

    def __init__(self,
                 address: Tuple[str, str],
                 store: BenjiStore,
                 read_only: bool = True,
                 discard_changes: bool = False) -> None:
        self.log = logging.getLogger(__package__)

        self.address = address
        self.store = store
        self.read_only = read_only
        self.discard_changes = discard_changes

        if asyncio.get_event_loop().is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
        self.loop = asyncio.get_event_loop()

    async def nbd_response(self,
                           writer: StreamWriter,
                           handle: int,
                           error: int = 0,
                           data: bytes = None) -> Generator[Any, None, None]:
        writer.write(struct.pack('>LLQ', self.NBD_SIMPLE_REPLY_MAGIC, error, handle))
        if data:
            writer.write(data)
        await writer.drain()

    async def nbd_opt_response(self,
                               writer: StreamWriter,
                               opt: int,
                               type: int,
                               data: bytes = None) -> Generator[Any, None, None]:
        writer.write(struct.pack(">QLLL", self.NBD_OPT_REPLY_MAGIC, opt, type, len(data) if data else 0))
        if data:
            writer.write(data)
        await writer.drain()

    async def handler(self, reader: StreamReader, writer: StreamWriter) -> Generator[Any, None, None]:
        data: Optional[bytes]
        try:
            host, port = writer.get_extra_info("peername")
            version: Optional[Version] = None
            cow_version: Optional[Version] = None
            self.log.info("Incoming connection from %s:%s." % (host, port))

            # Initial handshake
            writer.write(struct.pack(">QQH", self.INIT_PASSWD, self.CLISERV_MAGIC, self.NBD_HANDSHAKE_FLAGS))
            await writer.drain()

            data = await reader.readexactly(4)
            try:
                client_flags = struct.unpack(">L", data)[0]
            except struct.error:
                raise IOError("Handshake failed, disconnecting.")

            # The specification actually allows a client supporting "fixed" to not set this bit in its reply ("SHOULD").
            fixed = (client_flags & self.NBD_FLAG_FIXED_NEWSTYLE) != 0
            if not fixed:
                self.log.warning("Client did not signal fixed new-style handshake.")

            no_zeros = (client_flags & self.NBD_FLAG_NO_ZEROES) != 0
            if no_zeros:
                self.log.debug("Client requested NBD_FLAG_NO_ZEROES.")

            client_flags ^= self.NBD_FLAG_FIXED_NEWSTYLE | self.NBD_FLAG_NO_ZEROES
            if client_flags > 0:
                raise IOError("Handshake failed, unknown client flags %s, disconnecting." % (client_flags))

            # Negotiation phase
            version: VersionUid = None
            while True:
                header = await reader.readexactly(16)
                try:
                    (magic, opt, length) = struct.unpack(">QLL", header)
                except struct.error:
                    raise IOError("Negotiation failed: Invalid request, disconnecting.")

                if magic != self.CLISERV_MAGIC:
                    raise IOError("Negotiation failed: Bad magic number: %s." % magic)

                if length:
                    data = await reader.readexactly(length)
                    if len(data) != length:
                        raise IOError("Negotiation failed: %s bytes expected." % length)
                else:
                    data = None

                self.log.debug(f"[{host}:{port}]: opt={self.NBD_OPT_MAP.get(opt, 'unknown')}({opt}), length={length}, data={data!r}")

                if opt == self.NBD_OPT_EXPORT_NAME:
                    if not data:
                        raise IOError("Negotiation failed: No export name was provided.")

                    version_uid = VersionUid(data.decode("ascii"))
                    if not self.store.find_versions(version_uid=version_uid):
                        await self.nbd_opt_response(writer, opt, self.NBD_REP_ERR_INVALID)
                        continue

                    version = self.store.find_versions(version_uid=version_uid)[0]

                    export_flags = self.NBD_EXPORT_FLAGS
                    if self.read_only:
                        export_flags |= self.NBD_FLAG_READ_ONLY

                    # In case size is not a multiple of 4096 we extend it to the maximum support block
                    # size of 4096
                    size = math.ceil(version.size / 4096) * 4096
                    writer.write(struct.pack('>QH', size, export_flags))
                    if not no_zeros:
                        writer.write(b"\x00" * 124)
                    await writer.drain()

                    # Transition to transmission phase
                    break

                if opt == self.NBD_OPT_INFO or opt == self.NBD_OPT_GO:
                    if not data:
                        raise IOError("Negotiation failed: No data was provided.")

                    version_uid_len = struct.unpack('>L', data[:4])[0]
                    version_uid = VersionUid(data[4:4 + version_uid_len].decode('ascii'))
                    if not self.store.find_versions(version_uid=version_uid):
                        await self.nbd_opt_response(writer, opt, self.NBD_REP_ERR_INVALID)
                        continue

                    version = self.store.find_versions(version_uid=version_uid)[0]

                    # We ignore the rest of the data which may contain information requests and always answer with
                    # NBD_INFO_EXPORT.

                    export_flags = self.NBD_EXPORT_FLAGS
                    if self.read_only:
                        export_flags |= self.NBD_FLAG_READ_ONLY

                    # In case size is not a multiple of 4096 we extend it to the maximum support block
                    # size of 4096
                    size = math.ceil(version.size / 4096) * 4096
                    await self.nbd_opt_response(writer, opt, self.NBD_REP_INFO,
                                                struct.pack('>HQH', self.NBD_INFO_EXPORT, size, export_flags))

                    await self.nbd_opt_response(writer, opt, self.NBD_REP_ACK)

                    if opt == self.NBD_OPT_GO:
                        # Transition to transmission phase
                        break

                elif opt == self.NBD_OPT_LIST:
                    # Don't use version as a loop variable, so we don't conflict with the outer scope usage
                    for list_version in self.store.find_versions():
                        list_version_encoded = list_version.uid.encode("ascii")
                        await self.nbd_opt_response(writer, opt, self.NBD_REP_SERVER,
                                                    struct.pack(">L", len(list_version_encoded)) + list_version_encoded)

                    await self.nbd_opt_response(writer, opt, self.NBD_REP_ACK)

                elif opt == self.NBD_OPT_ABORT:
                    await self.nbd_opt_response(writer, opt, self.NBD_REP_ACK)

                    raise _NbdServerAbortedNegotiationError()
                else:
                    # We don't support any other option.
                    await self.nbd_opt_response(writer, opt, self.NBD_REP_ERR_UNSUP)

            self.log.info("[%s:%s] Negotiated export: %s." % (host, port, version_uid))
            if self.read_only:
                self.log.info("[%s:%s] Export is read only." % (host, port))
            else:
                self.log.info("[%s:%s] Export is read/write." % (host, port))
            self.store.open(version)

            self.log.info("[%s:%s] Version %s has been opened." % (host, port, version.uid))

            # Transmission phase
            while True:
                header = await reader.readexactly(28)
                try:
                    (magic, cmd, handle, offset, length) = struct.unpack(">LLQQL", header)
                except struct.error:
                    raise IOError("Invalid request, disconnecting.")

                if magic != self.NBD_REQUEST_MAGIC:
                    raise IOError("Bad magic number, disconnecting.")

                cmd_flags = cmd & self.NBD_CMD_MASK_FLAGS
                cmd = cmd & self.NBD_CMD_MASK_COMMAND

                self.log.debug(f"[{host}:{port}]: cmd={self.NBD_CMD_MAP.get(cmd, 'unknown')}({cmd}), cmd_flags={cmd_flags}, handle={handle}, offset={offset}, length={length}")

                # We don't support any command flags
                if cmd_flags != 0:
                    await self.nbd_response(writer, handle, error=self.EINVAL)
                    continue

                if cmd == self.NBD_CMD_DISC:
                    self.log.info("[%s:%s] disconnecting" % (host, port))
                    break

                elif cmd == self.NBD_CMD_WRITE:
                    data = await reader.readexactly(length)
                    if len(data) != length:
                        raise IOError("%s bytes expected, disconnecting." % length)

                    if self.read_only:
                        await self.nbd_response(writer, handle, error=self.EPERM)
                        continue

                    if not cow_version:
                        cow_version = self.store.create_cow_version(version)
                    try:
                        self.store.write(cow_version, offset, data)
                    except Exception as exception:
                        self.log.error("[%s:%s] NBD_CMD_WRITE: %s\n%s." %
                                       (host, port, exception, traceback.format_exc()))
                        await self.nbd_response(writer, handle, error=self.EIO)
                        continue

                    await self.nbd_response(writer, handle)

                elif cmd == self.NBD_CMD_READ:
                    try:
                        data = self.store.read(version, cow_version, offset, length)
                    except Exception as exception:
                        self.log.error("[%s:%s] NBD_CMD_READ: %s\n%s." % (host, port, exception, traceback.format_exc()))
                        await self.nbd_response(writer, handle, error=self.EIO)
                        continue

                    await self.nbd_response(writer, handle, data=data)

                elif cmd == self.NBD_CMD_FLUSH:
                    # Return success right away when we're read only or when we haven't written anything yet.
                    if self.read_only or not cow_version:
                        await self.nbd_response(writer, handle)
                        continue

                    try:
                        self.store.flush(cow_version)
                    except Exception as exception:
                        self.log.error("[%s:%s] NBD_CMD_FLUSH: %s\n%s." %
                                       (host, port, exception, traceback.format_exc()))
                        await self.nbd_response(writer, handle, error=self.EIO)
                        continue

                    await self.nbd_response(writer, handle)

                else:
                    self.log.warning("[%s:%s] Unknown cmd %s, ignoring." % (host, port, cmd))
                    await self.nbd_response(writer, handle, error=self.EINVAL)
                    continue

        except _NbdServerAbortedNegotiationError:
            self.log.info("[%s:%s] Client aborted negotiation." % (host, port))

        except (asyncio.IncompleteReadError, IOError) as exception:
            self.log.error("[%s:%s] %s" % (host, port, exception))

        finally:
            if cow_version:
                if self.discard_changes:
                    self.store.discard_cow_version(cow_version)
                else:
                    self.store.fixate_cow_version(cow_version)
            if version:
                self.store.close(version)
            writer.close()

    def serve_forever(self) -> None:
        addr, port = self.address

        loop = self.loop
        coro = asyncio.start_server(self.handler, addr, port)
        server = loop.run_until_complete(coro)

        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        loop.add_signal_handler(signal.SIGINT, loop.stop)

        loop.run_forever()

        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()

    def stop(self) -> None:
        if not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.loop.stop)
