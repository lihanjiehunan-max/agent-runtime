"""Probe loopback listeners without confusing closed TCP TIME_WAIT sockets."""
from contextlib import ExitStack
import socket


def check_loopback_ports(ports: list[int]) -> None:
    # Match ordinary server bind semantics. SO_REUSEADDR allows a closed socket's
    # TIME_WAIT state; unlike SO_REUSEPORT it does not share an active listener.
    with ExitStack() as stack:
        for port in ports:
            probe=stack.enter_context(socket.socket())
            probe.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            try:
                probe.bind(('127.0.0.1',port))
                probe.listen(1)
            except OSError as exc:
                raise OSError(exc.errno,f'Loopback port {port} is not available: {exc.strerror}') from exc
