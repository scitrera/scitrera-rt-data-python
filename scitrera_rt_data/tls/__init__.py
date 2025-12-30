import os
import re
import ssl
import tempfile
from typing import Optional


def _resolve_path(obj):
    """Return the path string if obj is path-like and exists, else None."""
    if obj is None:
        return None
    try:
        path = os.fspath(obj)  # raises TypeError if not path-like
    except TypeError:
        return None
    return path if os.path.exists(path) else None


def _require_exactly_one(name_a: str, val_a, name_b: str, val_b):
    """Raise ValueError unless exactly one of the two values is non-None."""
    a = val_a is not None
    b = val_b is not None
    if a ^ b:
        return
    raise ValueError(f"exactly one of '{name_a}' or '{name_b}' must be specified")


def _fix_pem_format(data: str) -> str:
    # Ensure newline after BEGIN line
    data = re.sub(
        r"(-----BEGIN [^-]+-----)\s*",  # match BEGIN header and any spaces/newlines
        r"\1\n",  # replace with header + single newline
        data
    )

    # Ensure newline before END line
    data = re.sub(
        r"\s*(-----END [^-]+-----)",  # match spaces/newlines before END
        r"\n\1",  # replace with single newline + END
        data
    )

    return data.strip() + "\n"  # strip outer junk, end with newline


def _to_bytes_pem(data) -> bytes:
    """
    Normalize PEM input (str|bytes) to bytes.

    If PEM data is given as a str, then also convert spaces to newlines to handle some weirdness that occurs when transitioning through
    environment variable type mechanisms.
    """
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return _fix_pem_format(data).encode('utf-8')
    raise TypeError("PEM data must be str or bytes")


def _write_temp_pem(pem_bytes: bytes, suffix: str = ".pem") -> str:
    """
    Write PEM bytes to a temp file (with secure permissions, 0600) and return its path.
    Caller is responsible for deleting the file.
    """
    # NamedTemporaryFile(delete=False) to allow Windows to reopen it later
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        # Restrict permissions to owner read/write (best effort on non-POSIX)
        try:
            os.fchmod(fd, 0o600)
        except AttributeError:
            pass  # not available on Windows; mkstemp already limits perms
        with os.fdopen(fd, "wb") as f:
            f.write(pem_bytes)
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        # On any failure, ensure the file doesn't linger
        try:
            os.close(fd)
        except Exception:
            pass
        try:
            os.remove(path)
        except Exception:
            pass
        raise
    return path


def mtls_client_ssl_context(
        *,
        ca_cert_path: Optional[str] = None,
        ca_data: Optional[bytes] = None,
        client_cert_path: Optional[str] = None,
        client_cert_data: Optional[bytes] = None,
        client_key_path: Optional[str] = None,
        client_key_data: Optional[bytes] = None,
        client_key_password: Optional[str] = None,
) -> ssl.SSLContext:
    """
    Build an mTLS client SSLContext.

    Exactly one of each pair must be provided:
      - (ca_cert_path | ca_data)
      - (client_cert_path | client_cert_data)
      - (client_key_path | client_key_data)

    When *_data is provided, a secure temporary PEM file is created and cleaned up.
    """
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = True

    # Validate the "exactly one" rule up front
    _require_exactly_one("ca_cert_path", ca_cert_path, "ca_data", ca_data)
    _require_exactly_one("client_cert_path", client_cert_path, "client_cert_data", client_cert_data)
    _require_exactly_one("client_key_path", client_key_path, "client_key_data", client_key_data)

    # Resolve any provided paths; None if not path-like or missing
    ca_cert_path = _resolve_path(ca_cert_path)
    client_cert_path = _resolve_path(client_cert_path)
    client_key_path = _resolve_path(client_key_path)

    # After resolution, ensure path variants are still valid (existence check is inside resolve_path)
    if ca_data is None and ca_cert_path is None:
        raise ValueError("ca_cert_path does not exist or is not path-like")
    if client_cert_data is None and client_cert_path is None:
        raise ValueError("client_cert_path does not exist or is not path-like")
    if client_key_data is None and client_key_path is None:
        raise ValueError("client_key_path does not exist or is not path-like")

    cleanup_ca_path = False
    cleanup_cert_path = False
    cleanup_key_path = False

    try:
        # CA store
        if ca_data is not None:
            ca_cert_path = _write_temp_pem(_to_bytes_pem(ca_data), suffix=".ca.pem")
            cleanup_ca_path = True

        # Load CA
        ssl_context.load_verify_locations(cafile=ca_cert_path)

        # Client cert
        if client_cert_data is not None:
            client_cert_path = _write_temp_pem(_to_bytes_pem(client_cert_data), suffix=".crt.pem")
            cleanup_cert_path = True  # remember to delete later

        # Client key
        if client_key_data is not None:
            client_key_path = _write_temp_pem(_to_bytes_pem(client_key_data), suffix=".key.pem")
            cleanup_key_path = True  # remember to delete later

        # Load client cert + key
        # password may be needed if the key is encrypted
        ssl_context.load_cert_chain(
            certfile=client_cert_path,
            keyfile=client_key_path,
            password=client_key_password,
        )

        return ssl_context

    finally:
        # Best-effort cleanup of any temp files we created (in order of descending security sensitivity)
        if cleanup_key_path and client_key_path:
            try:
                os.remove(client_key_path)
            except FileNotFoundError:
                pass
        if cleanup_cert_path and client_cert_path:
            try:
                os.remove(client_cert_path)
            except FileNotFoundError:
                pass
        if cleanup_ca_path and ca_cert_path:
            try:
                os.remove(ca_cert_path)
            except FileNotFoundError:
                pass
