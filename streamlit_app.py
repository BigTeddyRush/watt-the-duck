# streamlit_app.py
import duckdb
import pandas as pd
import streamlit as st
import altair as alt

# ---- CONFIG ----
DUCKDB_FILE = "dbt_project/energydata.duckdb"
# --------------------------------------------------

st.set_page_config(page_title="EnergyCharts Viewer", page_icon="⚡", layout="wide")
st.title("⚡ EnergyCharts Viewer")

# 1) Connect read-only (avoids lock issues)
con = duckdb.connect(DUCKDB_FILE, read_only=True)

# --------------------------------------------------
# CBET (Cross-Border Energy Transfers)
# --------------------------------------------------
st.header("CBET: Cross-Border Energy Transfers")
cbet = con.execute("SELECT * FROM analytics_core.core_energy_charts__cbet ORDER BY ts_utc").fetchdf()

if not cbet.empty:
    cbet["ts_utc"] = pd.to_datetime(cbet["ts_utc"]).dt.tz_convert("UTC").dt.tz_localize(None)

    # Melt wide -> long for plotting
    non_series = {"ts_utc", "country"}
    value_cols = [c for c in cbet.columns if c not in non_series and pd.api.types.is_numeric_dtype(cbet[c])]
    long = cbet.melt(id_vars=["ts_utc"], value_vars=value_cols,
                     var_name="country", value_name="value").dropna(subset=["value"])

    chart = (
        alt.Chart(long)
        .mark_line()
        .encode(
            x="ts_utc:T",
            y=alt.Y("value:Q", title="Trading Sum [MW]"),
            color="country:N",
            tooltip=["ts_utc:T", "country:N", alt.Tooltip("value:Q", format=".2f")]
        )
        .properties(height=400)
    )
    st.altair_chart(chart.interactive(), use_container_width=True)
    with st.expander("Raw data (CBET)"):
        st.dataframe(cbet.head(50), use_container_width=True)

# --------------------------------------------------
# DAP (Day-Ahead Prices)
# --------------------------------------------------
st.header("DAP: Day-Ahead Prices")
dap = con.execute("SELECT * FROM analytics_core.core_energy_charts__dap ORDER BY ts_utc").fetchdf()

if not dap.empty:
    dap["ts_utc"] = pd.to_datetime(dap["ts_utc"]).dt.tz_convert("UTC").dt.tz_localize(None)
    chart = (
        alt.Chart(dap)
        .mark_line()
        .encode(
            x="ts_utc:T",
            y=alt.Y("price:Q", title="Price [EUR/MWh]"),
            color="bzn:N",
            tooltip=["ts_utc:T", "bzn:N", alt.Tooltip("price:Q", format=".2f")]
        )
        .properties(height=400)
    )
    st.altair_chart(chart.interactive(), use_container_width=True)
    with st.expander("Raw data (DAP)"):
        st.dataframe(dap.head(50), use_container_width=True)


# --------------------------------------------------
# PPF (Production Forecasts: Day-Ahead)
# --------------------------------------------------
st.header("PPF: Day-Ahead Forecasts")
ppf = con.execute(
    "SELECT * FROM analytics_core.core_energy_charts__ppf "
    "WHERE forecast_type = 'day-ahead' ORDER BY ts_utc"
).fetchdf()

if not ppf.empty:
    ppf["ts_utc"] = pd.to_datetime(ppf["ts_utc"]).dt.tz_convert("UTC").dt.tz_localize(None)

    chart = (
        alt.Chart(ppf)
        .mark_line()
        .encode(
            x="ts_utc:T",
            y=alt.Y("forecast_values:Q", title="Forecast (MW)"),
            color="production_type:N",
            tooltip=["ts_utc:T", "production_type:N", "country:N", alt.Tooltip("forecast_values:Q", format=".1f")]
        )
        .properties(height=400)
    )
    st.altair_chart(chart.interactive(), use_container_width=True)

    with st.expander("Raw data (PPF - Day-Ahead)"):
        st.dataframe(ppf.head(50), use_container_width=True)



# --------------------------------------------------
# Combined: CBET (sum) + PPF (day-ahead) on one Y axis
# --------------------------------------------------
st.header("Combined: CBET (sum) + PPF (day-ahead)")

if not cbet.empty and not ppf.empty:
    # --- Ensure we have a CBET sum series ---
    if "sum" in cbet.columns and pd.api.types.is_numeric_dtype(cbet["sum"]):
        cbet_sum = cbet[["ts_utc", "sum"]].dropna()
    else:
        non_series = {"ts_utc", "sum", "country"}
        num_cols = [c for c in cbet.columns if c not in non_series and pd.api.types.is_numeric_dtype(cbet[c])]
        cbet_sum = cbet[["ts_utc"] + num_cols].copy()
        cbet_sum["sum"] = cbet_sum[num_cols].sum(axis=1, min_count=1)
        cbet_sum = cbet_sum[["ts_utc", "sum"]].dropna()

    # --- Prepare long format with both CBET + PPF ---
    cbet_long = cbet_sum.rename(columns={"sum": "value"}).assign(series="CBET sum")
    ppf_long = ppf.rename(columns={"forecast_values": "value", "production_type": "series"})
    ppf_long = ppf_long[["ts_utc", "series", "value"]]

    combined_df = pd.concat([cbet_long[["ts_utc", "series", "value"]], ppf_long])

    chart = (
        alt.Chart(combined_df)
        .mark_line()
        .encode(
            x=alt.X("ts_utc:T", title="Timestamp (UTC)"),
            y=alt.Y("value:Q", title="Value (MW)"),
            color=alt.Color("series:N", legend=alt.Legend(title="Series")),
            tooltip=["ts_utc:T", "series:N", alt.Tooltip("value:Q", format=".1f")]
        )
        .properties(height=420)
    )

    st.altair_chart(chart.interactive(), use_container_width=True)

    with st.expander("Raw combined data"):
        st.dataframe(combined_df.head(50), use_container_width=True)
else:
    st.info("Need both CBET and PPF data to render the combined chart.")



st.caption("Data source: DuckDB → analytics_core")
