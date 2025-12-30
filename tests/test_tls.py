import pytest
import os
import ssl
from scitrera_rt_data.tls import mtls_client_ssl_context, _fix_pem_format, _to_bytes_pem

def test_fix_pem_format():
    pem = "-----BEGIN CERTIFICATE-----  abc  -----END CERTIFICATE-----"
    fixed = _fix_pem_format(pem)
    assert fixed == "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"

def test_to_bytes_pem():
    pem_str = "-----BEGIN CERTIFICATE----- abc -----END CERTIFICATE-----"
    pem_bytes = _to_bytes_pem(pem_str)
    assert isinstance(pem_bytes, bytes)
    assert b"BEGIN CERTIFICATE" in pem_bytes
    
    # Test bytes input
    assert _to_bytes_pem(b"already bytes") == b"already bytes"
    
    with pytest.raises(TypeError):
        _to_bytes_pem(123)

def test_mtls_client_ssl_context_validation():
    # Test missing arguments
    with pytest.raises(ValueError, match="exactly one of 'ca_cert_path' or 'ca_data' must be specified"):
        mtls_client_ssl_context(
            client_cert_path="c.crt",
            client_key_path="c.key"
        )
        
    # Test both provided
    with pytest.raises(ValueError, match="exactly one of 'ca_cert_path' or 'ca_data' must be specified"):
        mtls_client_ssl_context(
            ca_cert_path="ca.crt",
            ca_data=b"ca",
            client_cert_path="c.crt",
            client_key_path="c.key"
        )

# Mocking SSLContext for tests that would require real certificates
from unittest.mock import MagicMock, patch

@patch("ssl.SSLContext")
@patch("os.path.exists", return_value=True)
def test_mtls_client_ssl_context_calls(mock_exists, mock_ssl_context_class):
    mock_context = MagicMock()
    mock_ssl_context_class.return_value = mock_context
    
    mtls_client_ssl_context(
        ca_cert_path="ca.crt",
        client_cert_path="client.crt",
        client_key_path="client.key"
    )
    
    mock_context.load_verify_locations.assert_called_once_with(cafile="ca.crt")
    mock_context.load_cert_chain.assert_called_once_with(
        certfile="client.crt",
        keyfile="client.key",
        password=None
    )
