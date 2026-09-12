"""Closed TCP connections are not listeners; active listeners still conflict."""
from pathlib import Path
import socket
import pytest


def check(ports):
    source=Path(__file__).resolve().parents[2]/'scripts/port_preflight.py'
    assert source.exists(), 'Port preflight must distinguish TIME_WAIT from a live listener'
    from scripts.port_preflight import check_loopback_ports
    check_loopback_ports(ports)


def test_active_listener_is_never_replaced_or_accepted():
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        listener.bind(('127.0.0.1',0));listener.listen(1)
        port=listener.getsockname()[1]
        with pytest.raises(OSError,match=str(port)):
            check([port])
        assert listener.getsockname()[1]==port


def test_closed_connection_in_time_wait_is_available_for_a_new_listener():
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        listener.bind(('127.0.0.1',0));listener.listen(1)
        port=listener.getsockname()[1]
        with socket.socket() as client:
            client.connect(('127.0.0.1',port))
            accepted,_=listener.accept()
            with accepted:
                accepted.shutdown(socket.SHUT_WR)
                assert client.recv(1)==b''
    check([port])


def test_distinct_ports_are_checked_together_and_released():
    sockets=[socket.socket() for _ in range(4)]
    try:
        for item in sockets:item.bind(('127.0.0.1',0))
        ports=[item.getsockname()[1] for item in sockets]
    finally:
        for item in sockets:item.close()
    check(ports)
    check(ports)
