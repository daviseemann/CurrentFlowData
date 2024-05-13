from utils.auth import authenticate
from utils.http_requests import get_ssl_context
import json
import pandas as pd 
import numpy as np

token = authenticate()
http = get_ssl_context()

test = http.request("GET",
        "https://integra.ons.org.br/api/energiaagora/GetBalancoEnergetico/null",
        headers={"Authorization": f"Bearer {token['access_token']}"},
    )
data = test.data.decode("utf-8")
data_json = json.loads(data)

df = pd.DataFrame(data_json)
print(df.head(10))