import importlib
import time
from datetime import datetime

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
            margin-bottom: -24px;
            font-size: 24px;
            color: #666666;
            font-size: 22px;
            font-weight: bold;
            opacity: 0.6;
        }
        
        div[class*='st-key-seoul_map_animation_'] {
            opacity: 0;
            border-radius: 16px 16px 0 0;
            width: 100%;
            max-height: 680px;
            margin-top: 8px;
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
            margin-top: -32px;
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
        "Choisir le polluant :",
        options=list(POLLUTANT_NAMES.keys()),
        format_func=lambda x: POLLUTANT_NAMES[x],
        index=0,
    )
    return pollutant


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
    print(hourly_average.iloc[:23].shape)
    # Max but only for the first day
    max_pollutant = hourly_average.iloc[:23][pollutant_col].max()

    return three_days_df, hourly_average, max_pollutant


def render_header(date_obj, pollutant_col, risk_str, risk_cls):
    """Renders the top header with the date and risk badge."""

    st.html(f"<div class='header_date'>{date_obj.strftime('%d/%m/%Y')}</div>")

    st.header(f"{current_day_fr} {date_obj.day} {current_month_fr}")

    col1, col2 = st.columns((3, 1))
    col1.write(
        f"Prévisions {"d'" if pollutant_col == 'o3_ppm' else 'de '}{POLLUTANT_LABELS[pollutant_col]} pour les 3 prochains jours."
    )
    col2.html(f'<div class="risk_status {risk_cls}">{risk_str}</div>')

    # st.space("small")


def render_map(three_days_df, pollutant_col="pm25_ugm3"):
    seoul_map = dashboard.get_daily_map(three_days_df, pollutant_col)

    # container = st.container(border=True)
    st.plotly_chart(
        seoul_map,
        key=f"seoul_map_animation_{pollutant_col}_{date.strftime('%Y%m%d')}",
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


# =====================================================================
# LOAD DATA
# ---------------------------------------------------------------------

dashboard = SeoulDashboard()
pollutant_col = add_pollutant_selector()

starting_date = datetime.strptime("2025-12-01", "%Y-%m-%d")
date = st.sidebar.date_input(
    "Choisir une date", value=starting_date.date(), key="picked_date"
)

current_day_fr = days_fr[date.weekday()]
current_month_fr = months_fr[date.month - 1]
current_date = date.strftime("%d/%m/%Y")

danger_string = "Chargement..."
danger_class = "waiting"

# ---------------------------------------------------------------------
# Get all the data we need

with st.spinner(text="In progress...", show_time=False, width="content"):
    three_days_df, hourly_average, max_pollutant_value = load_data(pollutant_col, date)

danger_string, danger_class = get_risk_level(
    max_pollutant_value, WHO_THRESHOLDS[pollutant_col]
)

# ---------------------------------------------------------------------
# Render everything at once

sidebar()

render_header(date, pollutant_col, danger_string, danger_class)

render_map(three_days_df, pollutant_col)

container = st.container(key="daily_pollutant_chart_container")
render_pollutant_chart(hourly_average, pollutant_col, container=container)

st.space("large")

# We need to cache the map somehow to avoid re-computation on every interaction
# @st.cache_resource
# def get_cached_map(_mapper_instance, data):
#     return _mapper_instance.get_daily_map(data)
