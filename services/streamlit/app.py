import importlib
import time
from datetime import datetime

import plotly.express as px
import streamlit as st

# # Trick to make it reload the module on every run
# import openaq_anomaly_prediction.core.base_map
# import openaq_anomaly_prediction.core.seoul_dashboard
# import openaq_anomaly_prediction.core.seoul_map
# importlib.reload(openaq_anomaly_prediction.core.seoul_dashboard)
# importlib.reload(openaq_anomaly_prediction.core.seoul_map)
# importlib.reload(openaq_anomaly_prediction.core.base_map)
from openaq_anomaly_prediction.core.base_map import BaseMap
from openaq_anomaly_prediction.core.seoul_dashboard import SeoulDashboard
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
 
        div[class*='st-key-seoul_map_animation_'] {
            border-radius: 16px 16px 0 0;
            width: 100%;
            max-height: 680px;
            background: #f6f7f8;
            animation: fadeIn 1000ms ease-in-out 100ms both!important;
        }
        .header_date {
            margin-bottom: -24px;
            font-size: 24px;
            color: #666666;
            font-size: 22px;
            font-weight: bold;
            opacity: 0.6;
        }
        
        .st-key-daily_pollutant_chart_container {
            border-radius: 0 0 16px 16px;
            border: 1px solid #e6e6e6;
            border-top-width: 0px;
            width: 100%;
            margin-top: -16px;
            padding: 28px 40px;
            background: #fff;
            animation: fadeIn 1000ms ease-in-out 200ms both!important;
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

starting_date = datetime.strptime("2025-12-01", "%Y-%m-%d")
date = st.sidebar.date_input(
    "Choisir une date", value=starting_date.date(), key="picked_date"
)


current_day_fr = days_fr[date.weekday()]
current_month_fr = months_fr[date.month - 1]
current_date = date.strftime("%d/%m/%Y")
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
    # option_map = {
    #     0: ":material/add:",
    #     1: ":material/zoom_in:",
    #     2: ":material/zoom_out:",
    #     3: ":material/zoom_out_map:",
    # }
    # selection = st.pills(
    #     "Tool",
    #     options=option_map.keys(),
    #     format_func=lambda option: option_map[option],
    #     selection_mode="single",
    # )
    # st.write(
    #     "Your selected option: "
    #     f"{None if selection is None else option_map[selection]}"
    # )

    pollutant = st.sidebar.selectbox(
        "Choisir le polluant :",
        options=["pm25_ugm3", "pm10_ugm3", "o3_ppm", "no2_ppm", "so2_ppm", "co_ppm"],
        index=0,
    )
    return pollutant


# =====================================================================
# MAIN PAGE
# ---------------------------------------------------------------------


def main():
    # st.title("Prévisions de PM2.5 (Séoul)")
    # st.space("small")
    st.html(f"<div class='header_date'>{current_date}</div>")
    st.header(f"{current_day_fr} {date.day} {current_month_fr}")
    st.write("In this section, we will explore the Altair cars dataset.")
    # st.markdown("*Test markdown*")


def load_data():
    with st.spinner(text="In progress...", show_time=False, width="content"):
        # daily_date = "2025-12-09"  # O3 peak (MOVING WAVES!)
        daily_df = dashboard.get_daily_data(date.strftime("%Y-%m-%d"))

        daily_average = (
            daily_df.groupby("oaq_datetimeto_local")[pollutant_col].mean().reset_index()
        )
        # col1.subheader("A wide column with a chart")
        # col1.line_chart(df)

        # col2.subheader("A narrow column with the data")
        # col2.write(df)

    # col1, col2 = st.columns([2, 1])

    # st.subheader("PM25", divider="gray")

    # row = col2.container(horizontal=False)
    # with row:
    #     st.metric("Line", 10, delta, chart_data=data, chart_type="line", border=True)
    #     # st.metric(
    #     #     "Area", 10, delta, chart_data=data, chart_type="area", border=True
    #     # )
    #     # st.metric(
    #     #     "Bar", 10, delta, chart_data=data, chart_type="bar", border=True
    #     # )

    st.space("small")
    load_map(daily_df, pollutant_col)
    container = st.container(key="daily_pollutant_chart_container")
    load_daily_pollutant_chart(daily_average, pollutant_col, container=container)


def load_daily_pollutant_chart(
    daily_average, pollutant_col="pm25_ugm3", container=None
):
    fig = dashboard.get_daily_pollutant_chart(daily_average, pollutant_col)
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


def load_map(daily_df, pollutant_col="pm25_ugm3"):
    seoul_map = dashboard.get_daily_map(daily_df, pollutant_col)

    # container = st.container(border=True)
    st.plotly_chart(
        seoul_map,
        key=f"seoul_map_animation_{pollutant_col}_{date.strftime('%Y%m%d')}",
    )


# =====================================================================
# LOAD DATA
# ---------------------------------------------------------------------
pollutant_col = add_pollutant_selector()


sidebar()
dashboard = SeoulDashboard()
main()
load_data()
st.space("large")
# We need to cache the map somehow to avoid re-computation on every interaction
# @st.cache_resource
# def get_cached_map(_mapper_instance, data):
#     return _mapper_instance.get_daily_map(data)
