"""
Author: Nitesh Tosniwal
Created Date: Saturday, September 23rd 2022, 10:42:00 am
"""

import json
import logging
import time
import enum
import urllib3
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Any, Tuple, Dict
from pydantic import BaseModel
import os

STRICT_SSL_VERIFY = False
PD_SHOW_ALL_COLS = True
DEBUG_MODE = True

logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if PD_SHOW_ALL_COLS:
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)

if not STRICT_SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Side(enum.Enum):
    BUY = "B"
    SELL = "S"

class Underlying(enum.Enum):
    PE = "PE"
    CE = "CE"
    NONE = ""


class PmOrderReqCreateMarket(BaseModel):
    client: str
    side: str
    trad_sym: Optional[str] = ""
    symbol: Optional[str] = ""
    expiry: Optional[str] = ""
    strike: Optional[float] = None
    underlying: str = ''
    shares: Optional[int] = 0
    lots: Optional[int] = 0
    amount: Optional[int] = 0


class PmOrderReqCreatePov(PmOrderReqCreateMarket):
    pov_target: float
    limit_price: Optional[float] = 0.0


class PmOrderReqCreate(PmOrderReqCreatePov):
    start_time: str
    end_time: str
    tactic: str
    pov_interval: Optional[int] = 1
    pov_max_order_lots: Optional[int] = 5
    htc_price: Optional[float] = 0.0
    broker: Optional[str] = ''


class PmOrderRequestsDTO(BaseModel):
    client: str
    symbol: str
    side: str
    quantity: int
    limit_price: float = 0.0
    price_offset: int = 0
    modification_offset: int = 9
    tactic: str = "TWAP"
    start_time: str
    end_time: str
    slice_freq: int = 10
    pov_target: Optional[float] = 0
    pov_interval: Optional[int] = 0
    pov_max_order_lots: Optional[int] = 1
    req_amount: Optional[int] = 0
    strike: Optional[str] = ""
    underlying: Optional[str] = ""
    broker: Optional[str] = ""
    temp_id: Optional[str] = ""


class PmOrderStopRequestsDTO(BaseModel):
    parent_order_id: int


class PmOrderModifyRequestsDTO(BaseModel):
    parent_order_id: int
    quantity: int
    limit_price: float
    start_time: str
    end_time: str
    pov_target: float


# AlgoEngine Client
class AlgoengineClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_api = base_url
        self.username = username
        self.password = password
        self._token = None
        self._token_exptime = None

    def get_access_token(self):
        """Get or refresh bearer token"""
        if self._token and self._token_exptime and datetime.now() < self._token_exptime:
            return self._token

        logging.info("Requesting access token...")
        login_url = f"{self.base_api}/auth/login"
        data = {"username": self.username, "password": self.password}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        response = requests.post(login_url, data=data, headers=headers, verify=STRICT_SSL_VERIFY)
        if not response.ok:
            logging.error(f"Token request failed: {response.status_code} {response.text}")
            response.raise_for_status()

        token = response.json().get("access_token", "")
        assert token, "Empty access token received"
        self._token = f"Bearer {token}"
        self._token_exptime = datetime.now() + timedelta(minutes=300)
        return self._token

    def _post(self, endpoint: str, payload: Any) -> Tuple[bool, Dict[str, Any]]:
        url = f"{self.base_api}{endpoint}"
        headers = {'Content-Type': 'application/json','Authorization': self.get_access_token()}
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers, verify=STRICT_SSL_VERIFY)
            response.raise_for_status()
            logging.info(f"POST {url} succeeded: {response.status_code}")
            return True, response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error during POST {url}: {http_err} - {response.text}")
        except requests.exceptions.RequestException as err:
            logging.error(f"Request failed during POST {url}: {err}")
        except Exception as ex:
            logging.error(f"Unexpected error during POST {url}: {ex}")
        return False, {}

    def _get(self, endpoint: str):
        url = f"{self.base_api}{endpoint}"
        headers = {'Content-Type': 'application/json', 'Authorization': self.get_access_token()}
        response = requests.get(url, headers=headers, verify=STRICT_SSL_VERIFY)
        if not response.ok:
            logging.error(f"GET {url} failed: {response.status_code}, {response.text}")
        return response

    def place_full_orders(self, orders: List[PmOrderRequestsDTO]):
        success, resp = self._post("/order_req/new_orders/create_orders", [o.model_dump() for o in orders])
        logging.info("place_full_orders: success=%s, response=%s", success, resp)

    def place_market_orders(self, orders: List[PmOrderReqCreateMarket]):
        success, resp = self._post("/order_req/new_orders/place_mkt_orders", [o.model_dump() for o in orders])
        logging.info("place_market_orders: success=%s, response=%s", success, resp)

    def place_pov_orders(self, orders: List[PmOrderReqCreatePov]):
        success, resp = self._post("/order_req/new_orders/place_quick_pov_orders", [o.model_dump() for o in orders])
        logging.info("place_pov_orders: success=%s, response=%s", success, resp)

    def parse_and_place_orders(self, orders: List[PmOrderReqCreate]):
        success, resp = self._post("/order_req/new_orders/parse_and_punch_orders", [o.model_dump() for o in orders])
        logging.info("parse_and_place_orders: success=%s, response=%s", success, resp)

    def cancel_orders(self, cx_order: PmOrderStopRequestsDTO):
        success, resp = self._post("/order_req/stop_order", cx_order.model_dump())
        logging.info("cancel_orders: success=%s, response=%s", success, resp)

    def modify_order(self, mod_order: PmOrderModifyRequestsDTO):
        success, resp = self._post("/order_req/modify_order", mod_order.model_dump())
        logging.info("modify_order: success=%s, response=%s", success, resp)

    def get_net_position(self):
        response = self._get("/snapshot/client_positions")
        df = pd.DataFrame(response.json())
        df['signed_qty'] = df.apply(lambda row: row['qty'] if row['side'] == 'BUY' else -row['qty'], axis=1)
        return df.groupby(['internal_accountid', 'symbol'])['signed_qty'].sum().reset_index()

    def get_order_status(self):
        response = self._get("/snapshot/parent_orders_status_all")
        df = pd.DataFrame(response.json().values())
        logging.info("\n" + df.to_string())

    def get_parent_orders(self):
        response = self._get("/snapshot/parent_orders")
        df = pd.DataFrame(response.json().values())
        logging.info("\n" + df.to_string())


# Configuration
class Config:
    ALGO_ENGINE_BASE_URL = "https://paperalgoengine.taracapitalpartners.com/api"
    ALGO_ENGINE_USER = ""
    ALGO_ENGINE_PASS = ""
    CLIENT_CODE = "MVQ1"
    BROKER = "PAPER"


if __name__ == "__main__":
    client = AlgoengineClient(
        base_url=Config.ALGO_ENGINE_BASE_URL,
        username=Config.ALGO_ENGINE_USER,
        password=Config.ALGO_ENGINE_PASS
    )
    token = client.get_access_token()
    assert token
    orders = [
        PmOrderRequestsDTO(
            client=Config.CLIENT_CODE,
            symbol="NIFTY_20250529_FUT",
            side=Side.BUY.value,
            quantity=10,
            limit_price=0.0,
            tactic="TWAP",
            start_time="09_15_00",
            end_time="15_15_00",
            broker=Config.BROKER
        )
    ]
    client.place_full_orders(orders)
