import contextlib
import os
import typing as ty
import sys
import json


def dbg_info(message: str, **kwargs: ty.Any) -> str:
    info: dict[str, ty.Any] = {}
    info.update(kwargs)
    info["message"] = message
    return json.dumps(info, indent=4)


@contextlib.contextmanager
def supress_output(
    *, stdout: bool = True, stderr: bool = False
) -> ty.Generator[None, None, None]:
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            if stdout:
                sys.stdout = devnull

            if stderr:
                sys.stderr = devnull

            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
