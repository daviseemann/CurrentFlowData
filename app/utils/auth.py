import ssl
import json
import urllib3
import os
from dotenv import load_dotenv


def authenticate():
    load_dotenv()

    # Load default SSL certificates and create an SSL context
    ctx = ssl.create_default_context()
    ctx.load_default_certs()
    # Allow connecting to a legacy server with unsafe renegotiation
    ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT

    # The API endpoint for authentication
    url = "https://integra.ons.org.br/api/autenticar"

    api_user = os.getenv("API_USER")
    api_password = os.getenv("API_PASSWORD")

    # Credentials payload
    payload = json.dumps({"usuario": api_user, "senha": api_password})

    # HTTP headers for the request
    headers = {
        "Origin": "https://portal-integra.ons.org.br",
        "Content-Type": "application/json",
    }

    # Create a PoolManager with the SSL context for making the request
    with urllib3.PoolManager(ssl_context=ctx) as http:
        # Assuming you've set up the request and SSL context as before

        try:
            response = http.request("POST", url, body=payload, headers=headers)
            print(response.status)

            # Decode the response data to JSON
            response_data = json.loads(response.data.decode("utf-8"))
            print(response_data)  # Print the entire response for debugging

            # Extract the access token, if present
            access_token = response_data.get("access_token")

            if access_token:
                print("Access Token:", access_token)
            else:
                print("Access token not found in the response.")

        except json.JSONDecodeError as e:
            print("Response is not in JSON format:", e)
        except KeyError as e:
            print("JSON response does not contain the expected key:", e)
        except urllib3.exceptions.HTTPError as e:
            print("HTTP error occurred:", e)
        except Exception as e:
            print("An error occurred:", e)
