"""Settings shared by every asset.

Compose injects the same `.env` the Rake tasks read, so the orchestrator and the
manual path cannot drift. Only two values are host-specific: the engine socket
and the workspace path, because bind mounts are resolved by the Podman daemon
and not by the Dagster container.
"""

import os
from pathlib import Path

WORKSPACE = Path(os.environ.get("PCL_WORKSPACE", "/workspace"))


def env(name: str, default: str | None = None) -> str:
    """Read a setting, naming it in the error when it is absent.

    An empty value counts as absent, so `env("X", "")` yields the empty default
    and `env("X")` fails loudly instead of handing back a blank credential.
    """
    value = os.environ.get(name)
    if value is None or value == "":
        if default is None:
            raise RuntimeError(f"Missing required setting {name}")
        return default
    return value
