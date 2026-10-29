"""Explicit loading of trusted plugin Python code into the client process."""
import hashlib
import importlib
import importlib.util
from pathlib import Path
import re
import sys
import tempfile
import threading

from packaging.specifiers import SpecifierSet

from .conf import read_global_conf, read_plugin_conf, read_remote_conf
from .version import __version__

_plugin_directory = tempfile.TemporaryDirectory(prefix="spyt-plugins-")
_loaded_plugins = {}
_load_lock = threading.Lock()


def load_plugin(client, plugin, spyt_version=None):
    """Load a trusted pure Python wheel or source ZIP into the current process.

    Download and verify the archive in process-local temporary storage, and return its
    local path. Dependencies must already be installed. Restart Python to
    replace an activated plugin or a module that is already installed or imported.
    """
    config = read_global_conf(client=client)
    if spyt_version is not None:
        config = read_remote_conf(config, spyt_version, client=client)
    python_conf = read_plugin_conf(config, plugin, client=client)["python"]
    checksum = python_conf["sha256"]
    modules = tuple(python_conf["modules"])
    requirement = python_conf["requires_spyt"]
    if re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
        raise ValueError(f"Plugin {plugin!r} must declare a lowercase SHA-256 checksum.")
    if not SpecifierSet(requirement).contains(__version__, prereleases=True):
        raise RuntimeError(f"Plugin {plugin!r} requires SPYT {requirement}; installed version is {__version__}.")
    identity = (checksum, modules)
    with _load_lock:
        previous = _loaded_plugins.get(plugin)
        if previous is not None:
            if previous[0] != identity:
                raise RuntimeError(f"Plugin {plugin!r} is already loaded with different Python code; restart Python.")
            return previous[1]
        for name in modules:
            if name in sys.modules or importlib.util.find_spec(name) is not None:
                raise RuntimeError(f"Plugin module {name!r} is already available; use a fresh Python environment.")
        target = Path(_plugin_directory.name) / f"{checksum}.zip"
        _download_archive(client, python_conf["path"], target, checksum)
        sys.path.insert(0, str(target))
        importlib.invalidate_caches()
        _loaded_plugins[plugin] = (identity, str(target))
        return str(target)


def _download_archive(client, path, target, checksum):
    with target.open("wb") as output:
        for chunk in client.read_file(path).chunk_iter():
            output.write(chunk)
    if hashlib.sha256(target.read_bytes()).hexdigest() != checksum:
        raise ValueError(f"Python archive checksum mismatch at {path}.")
