import asyncio
import json
from multiprocessing import AuthenticationError
import nest_asyncio
import pandas as pd
import websocket

from datetime import datetime, timedelta

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

nest_asyncio.apply()

DERIBIT_API_V2 = "wss://www.deribit.com/ws/api/v2"
DERIBIT_TEST_API_V2 = "wss://test.deribit.com/ws/api/v2"


class DeribitWS:
    def __init__(self, client_id, client_secret, live=False):
        self._url = DERIBIT_API_V2 if live else DERIBIT_TEST_API_V2
        self._ws = websocket.create_connection(self._url)
        self._auth_creds = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
        }

        # Validate credential
        self.__validate_credential()

    # Queuing orders from Deribit
    def get_market_order(self, instrument, amount, direction):
        params = {
            "instrument_name": instrument,
            "amount": amount,
            "type": "market",
        }

        if direction.lower() == "long":
            side = "buy"
        elif direction.lower() == "short":
            side = "sell"
        else:
            raise ValueError("direction must be long or short")

        response = self.api(json.dumps(self.__build_payload(f"private/{side}", params)))
        return response

    def get_limit_order(
        self, instrument, amount, direction, price, post_only, reduce_only
    ):
        params = {
            "instrument_name": instrument,
            "amount": amount,
            "type": "limit",
            "price": price,
            "post_only": post_only,
            "reduce_only": reduce_only,
        }
        if direction.lower() == "long":
            side = "buy"
        elif direction.lower() == "short":
            side = "sell"
        else:
            raise ValueError("direction must be long or short")

        response = self.api(json.dumps(self.__build_payload(f"private/{side}", params)))
        return response

    # market data methods
    def get_data(self, instrument, start, end, timeframe):
        params = {
            "instrument_name": instrument,
            "start_timestamp": start,
            "end_timestamp": end,
            "resolution": timeframe,
        }

        data = self.api(
            json.dumps(
                self.__build_payload("public/get_tradingview_chart_data", params)
            )
        )
        return data

    def get_orderbook(self, instrument, depth=5):
        params = {"instrument_name": instrument, "depth": depth}
        order_book = self.api(
            json.dumps(self.__build_payload("public/get_order_book", params)),
        )
        return order_book

    def get_quote(self, instrument):
        params = {"instrument_name": instrument}
        quote = self.api(json.dumps(self.__build_payload("public/ticker", params)))

        return quote["result"]["last_price"]

    def get_index_price(self, index):
        params = {"index_name": index}
        quote = self.api(
            json.dumps(self.__build_payload("public/get_index_price", params)),
        )

        return quote["result"]["index_price"]

    # account methods
    def get_account_summary(self, currency, extended=True):
        params = {"currency": currency, "extended": extended}

        summary = self.api(
            json.dumps(self.__build_payload("private/get_account_summary", params)),
        )
        return summary

    def get_positions(self, currency):
        params = {"currency": currency}
        positions = self.api(
            json.dumps(self.__build_payload("private/get_positions", params)),
        )
        return positions

    def available_instruments(self, currency, kind, expired=False):
        params = {"currency": currency, "kind": kind, "expired": expired}
        resp = self.api(
            json.dumps(self.__build_payload("public/get_instruments", params)),
        )
        instruments = [d["instrument_name"] for d in resp["result"]]
        return instruments

    def get_volatility_index_data(self, currency, start, end, timeframe):
        params = {
            "currency": currency,
            "start_timestamp": start,
            "end_timestamp": end,
            "resolution": timeframe,
        }

        vol_data = self.api(
            json.dumps(self.__build_payload("public/get_volatility_index_data", params))
        )
        return vol_data

    def get_user_trades_by_currency_time(
        self, currency, kind, start, end, count, include_old=True
    ):
        params = {
            "currency": currency,
            "kind": kind,
            "start_timestamp": start,
            "end_timestamp": end,
            "count": count,
            "include_old": include_old,
        }

        user_trades_data = self.api(
            json.dumps(
                self.__build_payload(
                    "private/get_user_trades_by_currency_and_time", params
                )
            )
        )
        return user_trades_data

    def api(self, msg):
        self._ws.send(msg)
        response = self._ws.recv()

        return json.loads(response)

    @staticmethod
    def json_to_dataframe(json_resp):
        res = json.loads(json_resp)
        df = pd.DataFrame(res["result"])
        df["ticks"] = df.ticks / 1000
        df["timestamp"] = [datetime.fromtimestamp(date) for date in df.ticks]

        return df

    @classmethod
    def unix_time_millis(self):
        return datetime.datetime.now().timestamp() * 1000

    def __validate_credential(self):
        response = self.api(json.dumps(self._auth_creds))
        if "error" in response.keys():
            raise AuthenticationError(f"Auth failed with error {response['error']}")

    def __build_payload(self, method, params):
        return {
            "jsonrpc": "2.0",
            "id": 0,
            "method": method,
            "params": params,
        }

    def get_user_trades(self, instrument):
        params = {
            "currency": instrument,
            "kind": "option",
            "count": 1,
            "include_old": False,
        }

        user_trades_data = self.api(
            json.dumps(
                self.__build_payload("private/get_user_trades_by_currency", params)
            )
        )
        return user_trades_data

    def get_user_trades_future(self, instrument):
        params = {
            "currency": instrument,
            "kind": "future",
            "count": 1,
            "include_old": False,
        }

        user_trades_data = self.api(
            json.dumps(
                self.__build_payload("private/get_user_trades_by_currency", params)
            ),
        )
        return user_trades_data

    def get_transaction_log(self, currency: str, delta: timedelta) -> dict:
        now = datetime.now()
        params = {
            "currency": currency,
            "start_timestamp": int((now - delta).timestamp() * 1000),
            "end_timestamp": int(now.timestamp() * 1000),
        }
        return self.api(
            json.dumps(self.__build_payload("private/get_transaction_log", params))
        )

    def get_subaccounts_details(self, currency: str) -> dict:
        params = {"currency": currency}
        return self.api(
            json.dumps(self.__build_payload("private/get_subaccounts_details", params))
        )
