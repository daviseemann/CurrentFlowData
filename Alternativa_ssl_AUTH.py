import ssl
import urllib3
import json
import os
from dotenv import load_dotenv

def create_ssl_context():
    """Create and configure an SSL context for HTTPS connections."""
    ctx = ssl.create_default_context()
    ctx.load_default_certs()
    # Allow connecting to a legacy server with unsafe renegotiation
    ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
    return ctx

def create_pool_manager(ssl_context):
    """Create a PoolManager with a given SSL context."""
    return urllib3.PoolManager(ssl_context=ssl_context)

def authenticate(api_user, api_password, url):
    """Authenticate with the API and return the response data."""
    headers = {
        "Origin": "https://portal-integra.ons.org.br",
        "Content-Type": "application/json"
    }
    payload = json.dumps({"usuario": api_user, "senha": api_password})

    # Create SSL context and PoolManager
    ssl_context = create_ssl_context()
    http = create_pool_manager(ssl_context)

    try:
        response = http.request("POST", url, body=payload, headers=headers)
        response_data = json.loads(response.data.decode("utf-8"))
        print("Authentication Response:", response_data)  # Printing the response data from authentication
        return response_data
    except Exception as e:
        print(f"An error occurred during authentication: {e}")
        return None

def main():
    load_dotenv()
    api_user = os.getenv("API_USER")
    api_password = os.getenv("API_PASSWORD")
    auth_url = "https://integra.ons.org.br/api/autenticar"

    token_response = authenticate(api_user, api_password, auth_url)
    if token_response and "access_token" in token_response:
        ssl_context = create_ssl_context()
        http = create_pool_manager(ssl_context)

        energy_url = "https://integra.ons.org.br/api/energiaagora/GetBalancoEnergetico/null"
        headers = {"Authorization": f"Bearer {token_response['access_token']}"}
        response = http.request("GET", energy_url, headers=headers)

        try:
            data_json = json.loads(response.data.decode("utf-8"))
            print("Energy Data Response:", data_json)  # Printing the response data from energy data request
        except json.JSONDecodeError:
            print("Failed to decode JSON from response.")
    else:
        print("Failed to obtain access token or access token is not in the response.")
        if token_response:
            print("API Response:", token_response)


teste = main()
print(teste)