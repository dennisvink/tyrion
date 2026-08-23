import errno
import os
import pty
import select
import subprocess
import sys
import time


def read_available(master, output, deadline, marker=None):
    while time.monotonic() < deadline:
        readable, _, _ = select.select([master], [], [], 0.05)
        if not readable:
            if marker is None:
                return False
            continue
        try:
            chunk = os.read(master, 4096)
        except OSError as exc:
            if exc.errno == errno.EIO:
                return False
            raise
        if not chunk:
            return False
        output.extend(chunk)
        if marker is not None and marker in output:
            return True
    return False


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: input_pty_driver.py <command> [args...]")

    master, slave = pty.openpty()
    process = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave, close_fds=True)
    os.close(slave)
    output = bytearray()
    try:
        deadline = time.monotonic() + 10
        if not read_available(master, output, deadline, b"Name: "):
            raise RuntimeError("input prompt did not appear")
        os.write(master, b"Arya\n\n")
        if not read_available(master, output, deadline, b"42"):
            raise RuntimeError("numeric input prompt did not appear")
        os.write(master, b"Sansa\n")
        while process.poll() is None:
            read_available(master, output, deadline)
            if time.monotonic() >= deadline:
                process.kill()
                raise RuntimeError("input PTY probe timed out")
        while read_available(master, output, time.monotonic() + 0.1):
            pass
    finally:
        os.close(master)

    if process.returncode != 0:
        sys.stderr.buffer.write(output)
        raise SystemExit(process.returncode)
    sys.stdout.buffer.write(output)


if __name__ == "__main__":
    main()
