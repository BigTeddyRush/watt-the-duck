# energy_charts_api.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import pandas as pd
from dlt.sources.helpers import requests


class EnergyChartsApiError(RuntimeError):
    """Raised when the Energy Charts API call or parsing fails."""


class EnergyChartsApi:
    """
    Thin client for https://api.energy-charts.info/.
    All methods return List[Dict[str, Any]] ready to be yielded into DLT resources.
    """

    def __init__(
        self,
        base_url: str = "https://api.energy-charts.info/",
        http: Any = requests,
        logger: Optional[callable] = None,
        timeout: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.http = http
        self.log = logger or (lambda *a, **k: None)
        self.timeout = timeout

    # --------- helpers ---------
    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path.lstrip('/')}"
        self.log(f"GET {url} params={params}")
        try:
            resp = self.http.get(url=url, params=params, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            # requests from dlt.helpers also raises RequestException-compatible errors
            raise EnergyChartsApiError(f"Failed request to {url}: {e}") from e
        try:
            return resp.json()
        except ValueError as e:
            raise EnergyChartsApiError(f"Invalid JSON from {url}: {e}") from e

    # --------- endpoints ---------
    def get_cross_border_electricity_trading(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        /cbet – Cross-border electricity trading.
        Expects params like: {"country": "...", "start": ..., "end": ...}
        """
        data = self._get("cbet", params=params)
        try:
            temp = {ptype["name"]: ptype["data"] for ptype in data["countries"]}
            temp["unix_seconds"] = data["unix_seconds"]
        except (KeyError, TypeError) as e:
            raise EnergyChartsApiError(f"Unexpected cbet payload structure: {e}") from e
        temp["country"] = params.get("country")
        df = pd.DataFrame(temp)
        return df.to_dict(orient="records")

    def get_cross_border_physical_flows(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        /cbpf – Cross-border physical flows.
        Expects params like: {"country": "...", "start": ..., "end": ...}
        """
        data = self._get("cbpf", params=params)
        try:
            temp = {ptype["name"]: ptype["data"] for ptype in data["countries"]}
            temp["unix_seconds"] = data["unix_seconds"]
        except (KeyError, TypeError) as e:
            raise EnergyChartsApiError(f"Unexpected cbpf payload structure: {e}") from e
        temp["country"] = params.get("country")
        df = pd.DataFrame(temp)
        return df.to_dict(orient="records")

    def get_public_power_forecast(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        /public_power_forecast – loops over production_types × forecast_types.
        Required keys in params:
          - country, start, end
          - production_types: List[str]
          - forecast_types: List[str] (used for all except production_type == 'load' where it's forced to ['day-ahead'])
        """
        url_path = "public_power_forecast"
        results: List[pd.DataFrame] = []

        production_types: Iterable[str] = params.get("production_types", [])
        default_forecast_types: Iterable[str] = params.get("forecast_types", [])

        base = {
            "country": params["country"],
            "start": params["start"],
            "end": params["end"],
        }

        for production_type in production_types:
            if production_type == "load":
                forecast_types = ["day-ahead"]
            else:
                forecast_types = list(default_forecast_types)

            for forecast_type in forecast_types:
                p = {**base, "production_type": production_type, "forecast_type": forecast_type}
                try:
                    data = self._get(url_path, params=p)
                except EnergyChartsApiError as e:
                    # skip this combination, continue others
                    self.log(f"[WARN] {e}")
                    continue

                # normalize: lists -> Series, supports missing keys
                try:
                    df = pd.DataFrame({k: pd.Series(v) for k, v in data.items()})
                except Exception as e:
                    self.log(f"[WARN] Failed to frame data for {p}: {e}")
                    continue

                df["production_type"] = production_type
                df["forecast_type"] = forecast_type
                df["country"] = base["country"]
                # drop columns that aren't needed if present
                for col in ("deprecated",):
                    if col in df.columns:
                        df = df.drop(columns=[col])
                results.append(df)

        if not results:
            raise EnergyChartsApiError(
                f"No public_power_forecast data available for {base['country']} in range "
                f"{base['start']}..{base['end']} with production_types={list(production_types)}"
            )

        return pd.concat(results, ignore_index=True).to_dict(orient="records")

    def get_day_ahead_price(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        /price – Day-ahead price (table named 'day_ahead_price' in DLT layer).
        Required keys in params:
          - start, end, bzn
        """
        data = self._get("price", params=params)

        # Build columns: lists as-is; scalars repeated to match list length
        try:
            # find length from the first list-valued field
            list_len = None
            for v in data.values():
                if isinstance(v, list):
                    list_len = len(v)
                    break
            if list_len is None:
                raise EnergyChartsApiError("Price response has no list-valued fields to infer row count.")

            cols = {}
            for k, v in data.items():
                if isinstance(v, list):
                    cols[k] = v
                else:
                    cols[k] = [v] * list_len

            df = pd.DataFrame(cols)
        except Exception as e:
            raise EnergyChartsApiError(f"Failed to normalize price payload: {e}") from e

        # enrich + clean
        if "bzn" not in df.columns:
            df["bzn"] = params.get("bzn")
        for col in ("deprecated", "license_info"):
            if col in df.columns:
                df = df.drop(columns=[col])

        return df.to_dict(orient="records")
