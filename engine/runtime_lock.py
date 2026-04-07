"""Runtime lock helpers to prevent duplicate bot instances."""

from __future__ import annotations

import atexit
import ctypes
import os
import subprocess
import time
from pathlib import Path


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False

    if os.name == "nt":
        # Windows-friendly PID liveness check.
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION,
            False,
            int(pid),
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _terminate_process_tree(pid: int) -> bool:
    """Terminate a process (and children where possible)."""
    if pid <= 0:
        return True

    if os.name == "nt":
        try:
            # /T kills child processes, /F forces termination.
            completed = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
            )
            # taskkill can still return non-zero for already-dead processes.
            # Wait briefly because Windows process teardown is not always immediate.
            for _ in range(30):
                if not _pid_is_running(pid):
                    return True
                time.sleep(0.1)

            if completed.returncode == 0:
                return not _pid_is_running(pid)
            return not _pid_is_running(pid)
        except Exception:
            return not _pid_is_running(pid)

    try:
        os.kill(pid, 15)
    except OSError:
        return True

    # Wait briefly for graceful termination.
    for _ in range(20):
        if not _pid_is_running(pid):
            return True
        time.sleep(0.1)

    # Fall back to hard kill.
    try:
        os.kill(pid, 9)
    except OSError:
        return True
    return not _pid_is_running(pid)


def acquire_single_instance_lock(lock_name: str, takeover_existing: bool = False) -> Path:
    """Create a pid lock file under logs/ and fail if another live process owns it."""
    lock_dir = Path("logs")
    lock_dir.mkdir(exist_ok=True)
    lock_path = lock_dir / f"{lock_name}.lock"

    # Atomically create the lock file to avoid startup races between processes.
    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, str(os.getpid()).encode("ascii"))
            finally:
                os.close(fd)
            break
        except FileExistsError:
            existing_pid = 0
            try:
                existing_pid = int(lock_path.read_text(encoding="ascii").strip() or "0")
            except Exception:
                existing_pid = 0

            if existing_pid and _pid_is_running(existing_pid):
                # If the lock contains our own PID (possible after PID reuse), do not self-terminate.
                if existing_pid == os.getpid():
                    try:
                        lock_path.unlink()
                    except Exception:
                        pass
                    continue

                if not takeover_existing:
                    raise RuntimeError(f"{lock_name} already running (pid={existing_pid})")

                terminated = _terminate_process_tree(existing_pid)
                if not terminated and _pid_is_running(existing_pid):
                    raise RuntimeError(
                        f"{lock_name} already running (pid={existing_pid}) and could not be stopped"
                    )

                # Give Windows a brief moment to release process handles.
                time.sleep(0.2)

            # Stale lock from a previous crash/kill.
            try:
                lock_path.unlink()
            except Exception:
                pass
    else:
        raise RuntimeError(f"Could not acquire runtime lock for {lock_name}")

    def _cleanup_lock():
        try:
            if not lock_path.exists():
                return
            current = lock_path.read_text(encoding="ascii").strip()
            if current == str(os.getpid()):
                lock_path.unlink()
        except Exception:
            pass

    atexit.register(_cleanup_lock)
    return lock_path
