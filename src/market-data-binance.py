import sys
import httpx

if __name__ == "__main__":
    API_URL = "https://api.binance.com"

    ENDPOINT = "/api/v3/klines"

    try:
        with httpx.Client(base_url=API_URL) as client:
            params = {
                "symbol": "BTCUSDT",
                "interval": "1d",
                "limit": 5,
            }
            res = client.get(ENDPOINT, params=params)

            print(f"Código de estado: {res.status_code}\n")
            print("Respuesta: ", res.json())

    except Exception as e:
        print("Algo salió mal\n")
        print(e)
        sys.exit(1)
