"""High-level Bitget exchange client with async retry helpers.

This module wraps the Bitget V2 REST API to provide a single entry point for
submitting trades, managing orders, and retrieving market state for USDT-M
perpetual futures.  It normalises retry behaviour, adds logging, and caches
contract metadata so that the trading agent can depend on predictable,
non-blocking IO.

Authentication uses HMAC-SHA256 as documented at:
https://www.bitget.com/api-doc/common/sign
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import socket
import time
from typing import Any

import requests
from requests.exceptions import RequestException

from src.config_loader import CONFIG

BITGET_BASE_URL = "https://api.bitget.com"
PRODUCT_TYPE = "USDT-FUTURES"
MARGIN_COIN = "USDT"


class BitgetAPI:
    """Facade around the Bitget V2 REST API with async convenience methods.

    The class owns API credentials, connection configuration, and provides
    coroutine helpers that keep retry semantics and logging consistent across
    the trading agent.
    """

    def __init__(self):
        """Initialise API credentials and validate required configuration.

        Raises:
            ValueError: If any of the three required Bitget credentials are
                absent from the configuration.
        """
        self.api_key = CONFIG.get("bitget_api_key")
        self.api_secret = CONFIG.get("bitget_api_secret")
        self.passphrase = CONFIG.get("bitget_passphrase")
        if not all([self.api_key, self.api_secret, self.passphrase]):
            raise ValueError(
                "BITGET_API_KEY, BITGET_API_SECRET, and BITGET_PASSPHRASE must be provided"
            )
        self.base_url = CONFIG.get("bitget_base_url") or BITGET_BASE_URL
        # Cached contract metadata keyed by symbol (e.g. "BTCUSDT")
        self._contracts_cache: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Response normalisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_list(data: dict | list, key: str = "entrustedList") -> list:
        """Extract a list from a Bitget response ``data`` payload.

        Bitget returns paginated lists under a ``key`` inside a dict, or
        occasionally as a bare list.  This helper handles both shapes.

        Args:
            data: The ``data`` field from a Bitget API response.
            key: Dict key whose value should be the list (default
                ``"entrustedList"``).

        Returns:
            The extracted list, or an empty list when not found.
        """
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get(key) or []
        return []

    # ------------------------------------------------------------------
    # Symbol helpers
    # ------------------------------------------------------------------

    def _symbol(self, asset: str) -> str:
        """Convert an asset ticker (e.g. ``BTC``) to a Bitget symbol (``BTCUSDT``)."""
        upper = asset.upper()
        if upper.endswith("USDT"):
            return upper
        return f"{upper}USDT"

    def _asset_from_symbol(self, symbol: str) -> str:
        """Convert a Bitget symbol (e.g. ``BTCUSDT``) back to a ticker (``BTC``)."""
        sym = symbol.upper()
        if sym.endswith("USDT"):
            return sym[:-4]
        return sym

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """Produce the base64-encoded HMAC-SHA256 signature for a request."""
        prehash = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self.api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _auth_headers(self, method: str, request_path: str, body: str = "") -> dict:
        """Build the full set of authentication headers for a Bitget request."""
        timestamp = str(int(time.time() * 1000))
        sign = self._sign(timestamp, method, request_path, body)
        return {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": sign,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
        }

    # ------------------------------------------------------------------
    # Low-level HTTP helpers (synchronous – called via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> dict:
        """Perform an authenticated GET request and return the parsed response.

        Args:
            path: API path without host (e.g. ``/api/v2/mix/market/ticker``).
            params: Optional query parameters appended to the path.

        Returns:
            Parsed JSON response dict.

        Raises:
            RuntimeError: When the Bitget ``code`` field is not ``"00000"``.
            requests.HTTPError: On non-2xx HTTP status.
        """
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            request_path = f"{path}?{qs}"
        else:
            request_path = path
        headers = self._auth_headers("GET", request_path)
        resp = requests.get(f"{self.base_url}{request_path}", headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "00000":
            raise RuntimeError(
                f"Bitget API error [{data.get('code')}]: {data.get('msg')} — path={path}"
            )
        return data

    def _post(self, path: str, body: dict | None = None) -> dict:
        """Perform an authenticated POST request and return the parsed response.

        Args:
            path: API path without host.
            body: Request body serialised to JSON.

        Returns:
            Parsed JSON response dict.

        Raises:
            RuntimeError: When the Bitget ``code`` field is not ``"00000"``.
            requests.HTTPError: On non-2xx HTTP status.
        """
        body_str = json.dumps(body or {})
        headers = self._auth_headers("POST", path, body_str)
        resp = requests.post(
            f"{self.base_url}{path}", headers=headers, data=body_str, timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "00000":
            raise RuntimeError(
                f"Bitget API error [{data.get('code')}]: {data.get('msg')} — path={path}"
            )
        return data

    # ------------------------------------------------------------------
    # Async retry helper
    # ------------------------------------------------------------------

    async def _retry(
        self,
        fn,
        *args,
        max_attempts: int = 3,
        backoff_base: float = 0.5,
        **kwargs,
    ):
        """Retry helper with exponential backoff and thread offloading.

        All callables are executed via :func:`asyncio.to_thread` so that
        blocking :mod:`requests` calls do not stall the event loop.

        Args:
            fn: Synchronous callable to invoke.
            *args: Positional arguments forwarded to ``fn``.
            max_attempts: Maximum number of attempts before re-raising.
            backoff_base: Initial delay in seconds, doubled after each failure.
            **kwargs: Keyword arguments forwarded to ``fn``.

        Returns:
            Result produced by ``fn``.

        Raises:
            Exception: Propagates any exception raised by ``fn`` after retries.
        """
        last_err: Exception | None = None
        for attempt in range(max_attempts):
            try:
                return await asyncio.to_thread(fn, *args, **kwargs)
            except (RequestException, ConnectionError, TimeoutError, socket.timeout) as e:
                last_err = e
                logging.warning(
                    "Bitget call failed (attempt %s/%s): %s", attempt + 1, max_attempts, e
                )
                await asyncio.sleep(backoff_base * (2**attempt))
                continue
            except (RuntimeError, ValueError, KeyError, AttributeError) as e:
                last_err = e
                logging.warning(
                    "Bitget call unexpected error (attempt %s/%s): %s",
                    attempt + 1,
                    max_attempts,
                    e,
                )
                # Non-network errors (e.g. API logic errors) are retried only
                # once: a second identical request is unlikely to succeed, so
                # we break early to surface the failure quickly.
                if attempt == 0:
                    await asyncio.sleep(backoff_base)
                    continue
                break
        raise last_err if last_err else RuntimeError("Bitget retry: unknown error")

    # ------------------------------------------------------------------
    # Contract metadata & size rounding
    # ------------------------------------------------------------------

    def _fetch_contract_info(self, symbol: str) -> dict:
        """Fetch and cache contract metadata for ``symbol``."""
        data = self._get(
            "/api/v2/mix/market/contracts",
            params={"symbol": symbol, "productType": PRODUCT_TYPE},
        )
        for c in data.get("data") or []:
            sym = c.get("symbol", "")
            if sym:
                self._contracts_cache[sym] = c
        return self._contracts_cache.get(symbol, {})

    def round_size(self, asset: str, amount: float) -> float:
        """Round order size to the contract's minimum lot increment.

        Args:
            asset: Asset ticker whose contract metadata we consult.
            amount: Desired quantity before rounding.

        Returns:
            ``amount`` rounded to the nearest ``sizeMultiplier`` increment.
        """
        symbol = self._symbol(asset)
        info = self._contracts_cache.get(symbol, {})
        try:
            multiplier = float(info.get("sizeMultiplier") or 0)
            if multiplier > 0:
                rounded = round(amount / multiplier) * multiplier
                multiplier_str = str(multiplier)
                # Use string-based decimal counting: sizeMultiplier values from
                # the Bitget contracts endpoint are always fixed-point numbers
                # (e.g. "0.001", "0.01", "1.0"), never scientific notation.
                # If an unexpected format is encountered the fallback
                # `round(amount, 6)` at the end of the method applies.
                if "." in multiplier_str:
                    decimals = len(multiplier_str.rstrip("0").split(".")[-1])
                else:
                    decimals = 0
                return round(rounded, decimals)
        except (ValueError, TypeError, KeyError):
            pass
        return round(amount, 6)

    # ------------------------------------------------------------------
    # Trading methods
    # ------------------------------------------------------------------

    async def place_buy_order(self, asset: str, amount: float, slippage: float = 0.01) -> dict:
        """Submit a market order to open a long position.

        Args:
            asset: Asset ticker (e.g. ``"BTC"``).
            amount: Contract quantity to buy before rounding.
            slippage: Accepted slippage (informational; Bitget market orders
                execute at best available price).

        Returns:
            Raw Bitget API response containing the new ``orderId``.
        """
        amount = self.round_size(asset, amount)

        def _place() -> dict:
            return self._post(
                "/api/v2/mix/order/place-order",
                {
                    "symbol": self._symbol(asset),
                    "productType": PRODUCT_TYPE,
                    "marginMode": "crossed",
                    "marginCoin": MARGIN_COIN,
                    "size": str(amount),
                    "side": "buy",
                    "tradeSide": "open",
                    "orderType": "market",
                },
            )

        return await self._retry(_place)

    async def place_sell_order(self, asset: str, amount: float, slippage: float = 0.01) -> dict:
        """Submit a market order to open a short position.

        Args:
            asset: Asset ticker.
            amount: Contract quantity to sell before rounding.
            slippage: Accepted slippage (informational).

        Returns:
            Raw Bitget API response containing the new ``orderId``.
        """
        amount = self.round_size(asset, amount)

        def _place() -> dict:
            return self._post(
                "/api/v2/mix/order/place-order",
                {
                    "symbol": self._symbol(asset),
                    "productType": PRODUCT_TYPE,
                    "marginMode": "crossed",
                    "marginCoin": MARGIN_COIN,
                    "size": str(amount),
                    "side": "sell",
                    "tradeSide": "open",
                    "orderType": "market",
                },
            )

        return await self._retry(_place)

    async def place_take_profit(
        self, asset: str, is_buy: bool, amount: float, tp_price: float
    ) -> dict:
        """Create a reduce-only TP trigger order for an existing position.

        Args:
            asset: Asset ticker.
            is_buy: ``True`` when the existing position is long.
            amount: Contract quantity to close at take-profit.
            tp_price: Trigger price for the take-profit order.

        Returns:
            Raw Bitget API response containing the new ``orderId``.
        """
        amount = self.round_size(asset, amount)
        hold_side = "long" if is_buy else "short"

        def _place() -> dict:
            return self._post(
                "/api/v2/mix/order/place-tpsl-order",
                {
                    "symbol": self._symbol(asset),
                    "productType": PRODUCT_TYPE,
                    "marginCoin": MARGIN_COIN,
                    "planType": "profit_plan",
                    "triggerPrice": str(tp_price),
                    "holdSide": hold_side,
                    "size": str(amount),
                },
            )

        return await self._retry(_place)

    async def place_stop_loss(
        self, asset: str, is_buy: bool, amount: float, sl_price: float
    ) -> dict:
        """Create a reduce-only SL trigger order for an existing position.

        Args:
            asset: Asset ticker.
            is_buy: ``True`` when the existing position is long.
            amount: Contract quantity to close at stop-loss.
            sl_price: Trigger price for the stop-loss order.

        Returns:
            Raw Bitget API response containing the new ``orderId``.
        """
        amount = self.round_size(asset, amount)
        hold_side = "long" if is_buy else "short"

        def _place() -> dict:
            return self._post(
                "/api/v2/mix/order/place-tpsl-order",
                {
                    "symbol": self._symbol(asset),
                    "productType": PRODUCT_TYPE,
                    "marginCoin": MARGIN_COIN,
                    "planType": "loss_plan",
                    "triggerPrice": str(sl_price),
                    "holdSide": hold_side,
                    "size": str(amount),
                },
            )

        return await self._retry(_place)

    async def cancel_order(self, asset: str, oid: str) -> dict:
        """Cancel a single order by ID, trying regular orders then plan orders.

        Args:
            asset: Asset ticker associated with the order.
            oid: Bitget order identifier to cancel.

        Returns:
            Raw Bitget API response.
        """
        symbol = self._symbol(asset)

        def _cancel() -> dict:
            try:
                return self._post(
                    "/api/v2/mix/order/cancel-order",
                    {"symbol": symbol, "productType": PRODUCT_TYPE, "orderId": oid},
                )
            except RuntimeError:
                # Might be a TP/SL plan order
                return self._post(
                    "/api/v2/mix/order/cancel-plan-order",
                    {"symbol": symbol, "productType": PRODUCT_TYPE, "orderId": oid},
                )

        return await self._retry(_cancel)

    async def cancel_all_orders(self, asset: str) -> dict:
        """Cancel every open and plan order for ``asset``.

        Args:
            asset: Asset ticker whose orders should be cancelled.

        Returns:
            Dict with ``status`` and ``cancelled_count`` fields.
        """
        symbol = self._symbol(asset)
        cancelled = 0
        try:
            # Cancel regular pending orders
            pending = await self._retry(
                self._get,
                "/api/v2/mix/order/orders-pending",
                {"symbol": symbol, "productType": PRODUCT_TYPE},
            )
            entries = pending.get("data") or {}
            order_list = self._extract_list(entries)
            for order in order_list:
                order_id = order.get("orderId")
                if order_id:
                    try:
                        await self._retry(
                            self._post,
                            "/api/v2/mix/order/cancel-order",
                            {"symbol": symbol, "productType": PRODUCT_TYPE, "orderId": order_id},
                        )
                        cancelled += 1
                    except (RuntimeError, ValueError, KeyError) as e:
                        logging.warning("Cancel order error: %s", e)

            # Cancel plan (TP/SL) orders
            plan_pending = await self._retry(
                self._get,
                "/api/v2/mix/order/orders-plan-pending",
                {"symbol": symbol, "productType": PRODUCT_TYPE},
            )
            plan_entries = plan_pending.get("data") or {}
            plan_list = self._extract_list(plan_entries)
            for order in plan_list:
                order_id = order.get("orderId")
                if order_id:
                    try:
                        await self._retry(
                            self._post,
                            "/api/v2/mix/order/cancel-plan-order",
                            {"symbol": symbol, "productType": PRODUCT_TYPE, "orderId": order_id},
                        )
                        cancelled += 1
                    except (RuntimeError, ValueError, KeyError) as e:
                        logging.warning("Cancel plan order error: %s", e)

            return {"status": "ok", "cancelled_count": cancelled}
        except (RuntimeError, ValueError, KeyError, ConnectionError) as e:
            logging.error("Cancel all orders error for %s: %s", asset, e)
            return {"status": "error", "message": str(e)}

    async def get_open_orders(self) -> list:
        """Fetch and normalise all open orders (regular + plan/TP/SL) across all assets.

        Returns:
            List of order dicts with ``coin``, ``oid``, ``isBuy``, ``sz``,
            ``px``, ``triggerPx``, and ``orderType`` keys.
        """
        orders: list[dict] = []

        # Regular pending orders
        try:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/order/orders-pending",
                {"productType": PRODUCT_TYPE},
            )
            entries = resp.get("data") or {}
            order_list = self._extract_list(entries)
            for o in order_list:
                orders.append(
                    {
                        "coin": self._asset_from_symbol(o.get("symbol", "")),
                        "oid": o.get("orderId"),
                        "isBuy": o.get("side", "").lower() == "buy",
                        "sz": float(o.get("size") or 0),
                        "px": float(o.get("price") or 0) or None,
                        "triggerPx": None,
                        "orderType": o.get("orderType"),
                    }
                )
        except (RuntimeError, ValueError, KeyError, ConnectionError) as e:
            logging.error("Get open orders error: %s", e)

        # Plan (TP/SL) pending orders
        try:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/order/orders-plan-pending",
                {"productType": PRODUCT_TYPE},
            )
            entries = resp.get("data") or {}
            plan_list = self._extract_list(entries)
            for o in plan_list:
                hold_side = o.get("holdSide", "long").lower()
                plan_type = o.get("planType", "")
                # Closing a long needs a sell; closing a short needs a buy.
                # `is_buy` describes the *closing* direction, which is the
                # opposite of the *hold* direction — hence True when short.
                is_buy = hold_side == "short"
                orders.append(
                    {
                        "coin": self._asset_from_symbol(o.get("symbol", "")),
                        "oid": o.get("orderId"),
                        "isBuy": is_buy,
                        "sz": float(o.get("size") or 0),
                        "px": None,
                        "triggerPx": float(o.get("triggerPrice") or 0) or None,
                        "orderType": "tp" if plan_type == "profit_plan" else "sl",
                    }
                )
        except (RuntimeError, ValueError, KeyError, ConnectionError) as e:
            logging.error("Get plan orders error: %s", e)

        return orders

    async def get_recent_fills(self, limit: int = 50) -> list:
        """Return recent trade fills across all USDT-M futures assets.

        Args:
            limit: Maximum number of fills to return (most recent first).

        Returns:
            List of fill dicts with ``coin``, ``isBuy``, ``sz``, ``px``, and
            ``time`` keys.
        """
        try:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/order/fills",
                {"productType": PRODUCT_TYPE},
            )
            raw = resp.get("data") or {}
            fill_list = self._extract_list(raw, key="fillList")
            fills = []
            for f in fill_list:
                fills.append(
                    {
                        "coin": self._asset_from_symbol(f.get("symbol", "")),
                        "isBuy": f.get("side", "").lower() == "buy",
                        "sz": float(f.get("fillSize") or 0),
                        "px": float(f.get("fillPrice") or 0),
                        "time": int(f.get("cTime") or 0),
                    }
                )
            return fills[-limit:]
        except (RuntimeError, ValueError, KeyError, ConnectionError, AttributeError) as e:
            logging.error("Get recent fills error: %s", e)
            return []

    def extract_oids(self, order_result: dict) -> list:
        """Extract order identifiers from a Bitget order placement response.

        Args:
            order_result: Raw response payload from :meth:`place_buy_order`,
                :meth:`place_sell_order`, :meth:`place_take_profit`, or
                :meth:`place_stop_loss`.

        Returns:
            List containing the ``orderId`` string when present.
        """
        oids: list[str] = []
        try:
            oid = (order_result.get("data") or {}).get("orderId")
            if oid:
                oids.append(str(oid))
        except (KeyError, TypeError, ValueError):
            pass
        return oids

    async def get_user_state(self) -> dict:
        """Retrieve account balance and enriched open-position data.

        Returns:
            Dict with ``balance`` (available USDT), ``total_value`` (equity),
            and ``positions`` (list of normalised position dicts).
        """
        # Account balance
        accounts_resp = await self._retry(
            self._get,
            "/api/v2/mix/account/accounts",
            {"productType": PRODUCT_TYPE},
        )
        accounts = accounts_resp.get("data") or []
        if isinstance(accounts, list):
            account = next(
                (a for a in accounts if str(a.get("marginCoin", "")).upper() == MARGIN_COIN),
                accounts[0] if accounts else {},
            )
        else:
            account = accounts

        balance = float(account.get("available") or 0.0)
        total_value = float(
            account.get("usdtEquity") or account.get("equity") or balance
        )

        # Open positions
        positions_resp = await self._retry(
            self._get,
            "/api/v2/mix/position/all-position",
            {"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN},
        )
        raw_positions = positions_resp.get("data") or []
        enriched: list[dict] = []
        for pos in raw_positions:
            total_size = float(pos.get("total") or 0)
            if total_size == 0:
                continue
            hold_side = pos.get("holdSide", "long").lower()
            # Positive szi = long position size; negative szi = short position size
            szi = total_size if hold_side == "long" else -total_size
            entry_px = float(pos.get("averageOpenPrice") or 0)
            liq_px = float(pos.get("liquidationPrice") or 0) or None
            pnl = float(pos.get("unrealizedPL") or 0)
            asset = self._asset_from_symbol(pos.get("symbol", ""))
            leverage_val = pos.get("leverage")
            enriched.append(
                {
                    "coin": asset,
                    "szi": szi,
                    "entryPx": entry_px,
                    "liquidationPx": liq_px,
                    "pnl": pnl,
                    "notional_entry": abs(szi) * entry_px,
                    "leverage": {
                        "type": pos.get("marginMode", "crossed"),
                        "value": int(float(leverage_val or 0)),
                    },
                }
            )

        return {"balance": balance, "total_value": total_value, "positions": enriched}

    async def get_current_price(self, asset: str) -> float:
        """Return the latest mark/last price for ``asset``.

        Args:
            asset: Asset ticker to query.

        Returns:
            Price as a float, or ``0.0`` when unavailable.
        """
        try:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/market/ticker",
                {"symbol": self._symbol(asset), "productType": PRODUCT_TYPE},
            )
            data = resp.get("data") or []
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict):
                item = data
            else:
                return 0.0
            return float(item.get("lastPr") or item.get("last") or item.get("close") or 0.0)
        except (RuntimeError, ValueError, KeyError, ConnectionError, TypeError) as e:
            logging.error("Get current price error for %s: %s", asset, e)
            return 0.0

    async def get_meta_and_ctxs(self) -> dict:
        """Return cached contract metadata for all USDT-M futures.

        Returns:
            Dict keyed by symbol (e.g. ``"BTCUSDT"``) mapping to contract
            metadata dicts as returned by the Bitget contracts endpoint.
        """
        if not self._contracts_cache:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/market/contracts",
                {"productType": PRODUCT_TYPE},
            )
            for c in resp.get("data") or []:
                sym = c.get("symbol", "")
                if sym:
                    self._contracts_cache[sym] = c
        return self._contracts_cache

    async def get_open_interest(self, asset: str) -> float | None:
        """Return current open interest for ``asset``.

        Args:
            asset: Asset ticker to query.

        Returns:
            Open interest as a float rounded to 2 decimals, or ``None``.
        """
        try:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/market/open-interest",
                {"symbol": self._symbol(asset), "productType": PRODUCT_TYPE},
            )
            data = resp.get("data") or {}
            if isinstance(data, list):
                data = data[0] if data else {}
            oi = data.get("amount") or data.get("openInterest") or data.get("size")
            return round(float(oi), 2) if oi is not None else None
        except (RuntimeError, ValueError, KeyError, ConnectionError, TypeError) as e:
            logging.error("OI fetch error for %s: %s", asset, e)
            return None

    async def get_funding_rate(self, asset: str) -> float | None:
        """Return the current funding rate for ``asset``.

        Args:
            asset: Asset ticker to query.

        Returns:
            Funding rate as a float rounded to 8 decimals, or ``None``.
        """
        try:
            resp = await self._retry(
                self._get,
                "/api/v2/mix/market/current-fund-rate",
                {"symbol": self._symbol(asset), "productType": PRODUCT_TYPE},
            )
            data = resp.get("data") or {}
            if isinstance(data, list):
                data = data[0] if data else {}
            rate = data.get("fundingRate")
            return round(float(rate), 8) if rate is not None else None
        except (RuntimeError, ValueError, KeyError, ConnectionError, TypeError) as e:
            logging.error("Funding rate error for %s: %s", asset, e)
            return None
