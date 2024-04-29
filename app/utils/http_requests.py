import ssl
import urllib3


def get_ssl_context():
    # Load default SSL certificates and create an SSL context
    ctx = ssl.create_default_context()
    ctx.load_default_certs()
    # Allow connecting to a legacy server with unsafe renegotiation
    ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
    return urllib3.PoolManager(ssl_context=ctx)
