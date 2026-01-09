import importlib
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

# Trick to make it reload the module on every run
import openaq_anomaly_prediction.core.base_map
import openaq_anomaly_prediction.core.seoul_dashboard
import openaq_anomaly_prediction.core.seoul_map

importlib.reload(openaq_anomaly_prediction.core.seoul_dashboard)
importlib.reload(openaq_anomaly_prediction.core.seoul_map)
importlib.reload(openaq_anomaly_prediction.core.base_map)
from openaq_anomaly_prediction.core.base_map import BaseMap
from openaq_anomaly_prediction.core.seoul_dashboard import (
    POLLUTANT_LABELS,
    POLLUTANT_NAMES,
    WHO_THRESHOLDS,
    SeoulDashboard,
)
from openaq_anomaly_prediction.core.seoul_map import SeoulMap

# =====================================================================
# STREAMLIT CONFIGURATION
# ---------------------------------------------------------------------

st.set_page_config(layout="wide")

st.markdown(
    """
    
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Google+Sans:ital,opsz,wght@0,17..18,400..700;1,17..18,400..700&display=swap');
        /*
            html, body, [class*="css"] {
                font-family: 'Google Sans Flex'!important, sans-serif;
                font-optical-sizing: auto;
                font-weight: <weight>;
                font-style: normal;
                font-variation-settings:
                    "slnt" 0,
                    "wdth" 100,
                    "GRAD" 0,
                    "ROND" 0;
                font-family: "Inter Tight", sans-serif;
                font-optical-sizing: auto;
                font-weight: <weight>;
                font-style: normal;
            }
        */
        .stAppHeader { background: none; }
            
        @keyframes fadeIn {
            0% { opacity: 0; }
            100% { opacity: 1; }
        }
        [data-testid="stPlotlyChart"] {
            animation: fadeIn 500ms ease-in-out both;
        }
        
        .block-container {
            /*border: 1px solid black;*/
            width: 100%;
            height: 100%;
            max-width: 1280px;
            min-width: 1024px!important;
            margin: auto;
            
        }

        .header_date {
            position: relative;
            display: inline-block;
            margin-bottom: -24px;
            font-size: 24px;
            color: rgba(0,0,0, 0.4);
            font-size: 22px;
            font-weight: bold;
        }
        .header_date span {
            position: absolute;
            top: -8px;
            right: -44px;
            font-size: 28px;
            color: rgba(0, 0, 0, 1);
        }
        
                .risk_status {
            opacity: 0;
            display: inline-block;
            text-align: right;
            color: #fff;
            font-size: 20px;
            font-weight: bold;
            background: #c19734;
            border: 6px solid white;
            float: right;
            margin-top: -36px;
            padding: 10px 24px;
            border-radius: 1000px;
            /* position: absolute;
            top: -48px;
            right: 0;*/
        }
        
        .risk_status.none {
            opacity: 1;
            background: #4caf50;
        }
        .risk_status.low {
            opacity: 1;
            background: #dd952b;
            text-transform: uppercase;
        }
        .risk_status.moderate {
            opacity: 1;
            background: #f73c2e;
            text-transform: uppercase;
        }
        .risk_status.high {
            opacity: 1;
            background: #000000;
            border-color: #ff3030;
            box-shadow: 0 0 15px #ff3030, inset 0 0 10px #ff3030;
            text-transform: uppercase;
            transform: translatey(-3px);
        }
        
        div[class*='st-key-seoul_map_animation_'] {
            opacity: 0;
            border-radius: 16px 16px 0 0;
            width: 100%;
            max-height: 680px;
            margin-top: 12px;
            background: #f6f7f8;
            animation: fadeIn 1000ms ease-in-out 300ms both!important;
        }
        
        .st-key-daily_pollutant_chart_container {
            opacity: 0;
            border-radius: 0 0 16px 16px;
            border: 1px solid #e6e6e6;
            border-top-width: 0px;
            width: 100%;
            padding: 28px 40px;
            margin-top: -16px;
            background: #fff;
            animation: fadeIn 1000ms ease-in-out 400ms both!important;
        }
        
        .st-key-anomalies_container {
            opacity: 0;
            border-radius: 16px;
            border: 1px solid #e6e6e6;
            width: 100%;
            margin-top: 12px;
            padding: 0;
            background: #fff;
            animation: fadeIn 1000ms ease-in-out 400ms both!important;
        }
        .st-key-anomaly_map {
            border-radius: 16px;
            width: 100%;
            max-height: 480px;
            background: #f6f7f8;
        }

    </style>
    """,
    unsafe_allow_html=True,
)


# =====================================================================
# NAVIGATION
# ---------------------------------------------------------------------

days_fr = [
    "Lundi",
    "Mardi",
    "Mercredi",
    "Jeudi",
    "Vendredi",
    "Samedi",
    "Dimanche",
]
months_fr = [
    "Janvier",
    "Février",
    "Mars",
    "Avril",
    "Mai",
    "Juin",
    "Juillet",
    "Août",
    "Septembre",
    "Octobre",
    "Novembre",
    "Décembre",
]


# date_str = f"{current_day_fr} {first_datetime.strftime('%d/%m')}"

# st.sidebar.title(f"{current_day_fr}")
# st.sidebar.write(current_date)

# st.write(st.session_state.picked_date)


def sidebar():
    # pg = st.sidebar.title("Paramètres")
    pass


# =====================================================================
# UI ELEMENTS
# ---------------------------------------------------------------------


def add_pollutant_selector():
    pollutant = st.sidebar.selectbox(
        "Polluant :",
        options=list(POLLUTANT_NAMES.keys()),
        format_func=lambda x: POLLUTANT_NAMES[x],
        index=0,
    )
    return pollutant


MAP_STYLES = {
    "basic": "Défaut",
    "satellite": "Satellite",
    "satellite-streets": "Satellite Streets",
    "dark": "Mode Sombre",
    "light": "Mode Clair",
}


def add_map_style_selector():
    map_style = st.sidebar.selectbox(
        "Style de carte :",
        options=list(MAP_STYLES.keys()),
        format_func=lambda x: MAP_STYLES[x],
        index=0,
    )
    # map_style = st.sidebar.checkbox("Vue satellite", value=False)
    return map_style


# =====================================================================
# MAIN PAGE
# ---------------------------------------------------------------------


def get_risk_level(value, threshold_dict):
    """Pure logic function: Returns string and class based on value."""
    if value >= threshold_dict["high"]:
        return "DANGER MORTEL", "high"
    elif value >= threshold_dict["moderate"]:
        return "Danger immédiat", "moderate"
    elif value >= threshold_dict["low"]:
        return "Vigilance", "low"
    else:
        return "Aucun danger", "none"


@st.cache_data(show_spinner=False)
def load_data(pollutant_col, date):
    three_days_df = dashboard.get_daily_data(date.strftime("%Y-%m-%d"))

    hourly_average = (
        three_days_df.groupby("oaq_datetimeto_local")
        .agg(
            **{
                pollutant_col: (pollutant_col, "mean"),
                f"{pollutant_col}_pred": (f"{pollutant_col}_pred", "mean"),
                "is_anomaly": ("is_anomaly", "max"),
            }
        )
        .reset_index()
    )

    # Max but only for the first day
    max_pollutant = hourly_average.iloc[:23][pollutant_col].max()

    return three_days_df, hourly_average, max_pollutant


@st.cache_data(show_spinner=False)
def load_anomalies():
    anomalies = dashboard.get_anomalies()

    return anomalies.copy()


def load_full_history(pollutant_col):
    history = dashboard.get_full_history(pollutant_col)

    return history.copy()


# 2. Filtering Logic
def get_30_day_window(df, date_ref, days=30, date_col="ds"):
    """
    Returns a 30-day slice ending at date_ref.
    If date_ref is too close to the start/end of data,
    it snaps to the first or last 30 days available.
    """

    # anomalies["ds"] = pd.to_datetime(anomalies["ds"])
    # daily_anomalies = anomalies[
    #     anomalies["ds"].dt.date == st.session_state["selected_date"]
    # ]
    # anomalies_unique_dt = daily_anomalies["ds"].unique().tolist()

    if df.empty:
        return df

    # Normalize input to pandas Timestamp for easy comparison
    target_date = pd.to_datetime(date_ref)

    # Get dataset boundaries
    min_date = df[date_col].min()
    max_date = df[date_col].max()

    # ---------------------------------------------------------
    # TIMEZONE FIX: Match target_date to the dataframe's timezone
    # ---------------------------------------------------------
    if min_date.tz is not None:
        # If the dataframe is TZ-aware but target is naive, localize target
        if target_date.tz is None:
            target_date = target_date.tz_localize(min_date.tz)
        # If both are aware but different, convert target to match DF
        else:
            target_date = target_date.tz_convert(min_date.tz)
    else:
        # If dataframe is naive but target is aware, make target naive
        if target_date.tz is not None:
            target_date = target_date.tz_localize(None)

    # Default window: [target - 30 days, target]
    start_window = target_date - timedelta(days=days)
    end_window = target_date

    # --- GAP LOGIC ---

    # Case 1: Left Gap (Target is too early)
    # If the calculated start is before the data exists, snap to the START.
    if start_window < min_date:
        start_window = min_date
        end_window = min_date + timedelta(days=days)

    # Case 2: Right Gap (Target is too late)
    # If the target is after the data ends, snap to the END.
    elif end_window > max_date:
        end_window = max_date
        start_window = max_date - timedelta(days=days)

    # 3. Apply the mask
    mask = (df[date_col] >= start_window) & (df[date_col] <= end_window)
    return df.loc[mask].copy()


def render_header(date_obj, pollutant_col, risk_str, risk_cls):
    """Renders the top header with the date and risk badge."""

    if risk_cls == "high":
        risk_emoji = "☢️"
    elif risk_cls == "moderate":
        risk_emoji = "🔥"
    elif risk_cls == "low":
        risk_emoji = "📢"
    else:
        risk_emoji = "🍃"

    st.html(f"""<div class='header_date'>
        {date_obj.strftime("%d/%m/%Y")}
        <!--<span>{risk_emoji}</span>-->
    </div>""")

    st.header(f"{current_day_fr} {date_obj.day} {current_month_fr} {risk_emoji}")

    col1, col2 = st.columns((2, 1))
    col1.write(
        f"Prévisions {"d'" if pollutant_col == 'o3_ppm' else 'de '}{POLLUTANT_LABELS[pollutant_col]} pour les 3 prochains jours."
    )
    col2.html(f'<div class="risk_status {risk_cls}">{risk_str}</div>')

    # st.space("small")


def render_map(three_days_df, pollutant_col="pm25_ugm3", map_style="basic"):
    seoul_map = dashboard.get_daily_map(
        three_days_df, pollutant_col, map_style=map_style
    )

    st.plotly_chart(
        seoul_map,
        key=f"seoul_map_animation_{pollutant_col}_{st.session_state['selected_date'].strftime('%Y%m%d')}",
        config={"displayModeBar": False, "staticPlot": False},
    )


def render_pollutant_chart(hourly_average, pollutant_col="pm25_ugm3", container=None):
    fig = dashboard.get_daily_pollutant_chart(hourly_average, pollutant_col)
    if container:
        container.plotly_chart(
            fig,
            key="daily_pollutant_chart",
            config={"displayModeBar": False, "staticPlot": False},
        )
    else:
        st.plotly_chart(
            fig,
            key="daily_pollutant_chart",
            config={"displayModeBar": False, "staticPlot": False},
        )


def render_anomalies(datetime, anomalies_df, three_days_df, map_style="basic"):
    """Renders the top header with the date and risk badge."""

    # container = st.container(key="anomalies_container")

    col1, col2 = st.columns((3, 2))

    col1.plotly_chart(
        dashboard.get_anomaly_map(anomalies_df, datetime, map_style=map_style),
        key="anomaly_map",
        config={"displayModeBar": False, "staticPlot": False, "scrollZoom": False},
    )
    col2.plotly_chart(
        dashboard.get_anomaly_barchart(anomalies_df, datetime),
        key="anomaly_barchart",
        config={"displayModeBar": False, "staticPlot": True},
    )


def render_history(anomalies_df, pollutant_col, show_anomalies=True):
    st.plotly_chart(
        dashboard.get_pollutant_history_chart(
            anomalies_df,
            pollutant_col,
            st.session_state["selected_date"],
            show_anomalies=show_anomalies,
        ),
        key="history_map",
        config={"displayModeBar": False, "staticPlot": False, "scrollZoom": False},
    )


# =====================================================================
# LOAD DATA
# ---------------------------------------------------------------------

# st.logo(
#     """<svg xmlns="http://www.w3.org/2000/svg" width="900" height="600" viewBox="-72 -48 144 96"><path fill="#fff" d="M-72-48v96H72v-96z"/><g stroke="#000" stroke-width="4"><path transform="rotate(33.69006752598)" d="M-50-12v24m6 0v-24m6 0v24m76 0V1m0-2v-11m6 0v11m0 2v11m6 0V1m0-2v-11"/><path transform="rotate(-33.69006752598)" d="M-50-12v24m6 0V1m0-2v-11m6 0v24m76 0V1m0-2v-11m6 0v24m6 0V1m0-2v-11"/></g><g transform="rotate(33.69006752598)"><path fill="#cd2e3a" d="M12 0a18 18 0 11-36 0 24 24 0 1148 0"/><path fill="#0047a0" d="M0 0a12 12 0 1124 0 24 24 0 11-48 0 12 12 0 1024 0"/></g></svg>""",
#     size="large",
#     # icon_image=LOGO_URL_SMALL,
# )

st.sidebar.title("Séoul")
dashboard = SeoulDashboard()
pollutant_col = add_pollutant_selector()

starting_date = datetime.strptime("2025-12-01", "%Y-%m-%d")


# Initialize the date in session state if it doesn't exist
if "selected_date" not in st.session_state:
    st.session_state["selected_date"] = starting_date


st.sidebar.date_input("Date :", value=starting_date.date(), key="selected_date")

map_style = add_map_style_selector()

# print(f"Session selected date: {st.session_state['selected_date']}")

current_day_fr = days_fr[st.session_state["selected_date"].weekday()]
current_month_fr = months_fr[st.session_state["selected_date"].month - 1]
current_date = st.session_state["selected_date"].strftime("%d/%m/%Y")

danger_string = "Chargement..."
danger_class = "waiting"

# ---------------------------------------------------------------------
# Get all the data we need

with st.spinner(text="Chargement...", show_time=False, width="content"):
    three_days_df, hourly_average, max_pollutant_value = load_data(
        pollutant_col, st.session_state["selected_date"]
    )

danger_string, danger_class = get_risk_level(
    max_pollutant_value, WHO_THRESHOLDS[pollutant_col]
)

# ---------------------------------------------------------------------
# Render everything at once

sidebar()

render_header(
    st.session_state["selected_date"], pollutant_col, danger_string, danger_class
)

render_map(three_days_df, pollutant_col, map_style)

container = st.container(key="daily_pollutant_chart_container")
render_pollutant_chart(hourly_average, pollutant_col, container=container)
st.space("small")

with st.spinner(text="Chargement...", show_time=False, width="content"):
    anomalies = load_anomalies()
    anomalies["ds"] = pd.to_datetime(anomalies["ds"])

    daily_anomalies = anomalies[
        anomalies["ds"].dt.date == st.session_state["selected_date"]
    ]
    anomalies_unique_dt = daily_anomalies["ds"].unique().tolist()

col1, col2 = st.columns((3, 1), vertical_alignment="bottom")


if daily_anomalies.empty:
    col1.header("Aucune anomalie détectée")
    st.write("Aucune anomalie n'a été détectée pour cette date.")

else:
    col1.header(f"Anomalies détectées ({len(anomalies_unique_dt)})")
    option = col2.selectbox(
        "Selectionner une heure",
        (dt.strftime("%Y-%m-%d %H:%M") for dt in anomalies_unique_dt),
        label_visibility="collapsed",
    )
    st.space("small")
    render_anomalies(
        st.session_state["selected_date"],
        daily_anomalies[daily_anomalies["ds"] == option],
        three_days_df,
        map_style=map_style,
    )


st.space("large")

st.header(f"Données historiques ({POLLUTANT_NAMES[pollutant_col]})")

col1, col2 = st.columns((3, 1), vertical_alignment="bottom")

if col1.button("Charger les données historiques"):
    with st.spinner(text="Chargement...", show_time=False, width="content"):
        history = load_full_history(pollutant_col)

    daily_average = (
        history.groupby("oaq_datetimeto_local")
        .agg(
            **{
                pollutant_col: (pollutant_col, "mean"),
                f"{pollutant_col}_pred": (f"{pollutant_col}_pred", "mean"),
                "is_anomaly": ("is_anomaly", "max"),
            }
        )
        .reset_index()
    )

    # show_anomalies = col2.toggle(
    #     "Montrer les anomalies", key="show_historical_anomalies", value=True
    # )

    # daily_average = get_30_day_window(
    #     daily_average,
    #     st.session_state["selected_date"],
    #     days=30,
    #     date_col="oaq_datetimeto_local",
    # )
    st.space("small")
    render_history(daily_average, pollutant_col, show_anomalies=True)

st.space("large")

st.header(f"Données brutes")
st.space("small")
st.dataframe(three_days_df)
st.space("large")
# We need to cache the map somehow to avoid re-computation on every interaction
# @st.cache_resource
# def get_cached_map(_mapper_instance, data):
#     return _mapper_instance.get_daily_map(data)
