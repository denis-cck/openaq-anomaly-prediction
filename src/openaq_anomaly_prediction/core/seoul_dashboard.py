import os
import time
from datetime import timedelta

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
from plotly.graph_objs import Figure as PlotlyFigure

from openaq_anomaly_prediction.config import Configuration as cfg
from openaq_anomaly_prediction.core.seoul_map import SeoulMap
from openaq_anomaly_prediction.core.xgboost import CustomXGBoostModel
from openaq_anomaly_prediction.load.gcp import bq

WHO_THRESHOLDS = {
    "pm25_ugm3": {
        "low": 7.5,  # 50% of WHO limit (Safe zone)
        "moderate": 15,  # WHO 24h Guideline (Alert triggers here)
        "high": 37.5,  # Interim Target 2 (Danger zone)
    },
    "pm10_ugm3": {
        "low": 22.5,  # 50% of WHO limit
        "moderate": 45,  # WHO 24h Guideline
        "high": 125,  # Interim Target 1
    },
    "o3_ppm": {
        "low": 0.032,  # 50% of limit
        "moderate": 0.064,  # Approx 8h limit
        "high": 0.100,  # High concentration
    },
    "no2_ppm": {
        "low": 0.026,  # 50% of limit
        "moderate": 0.053,  # US EPA Annual Standard equivalent
        "high": 0.090,  # Approx 1h limit
    },
    "so2_ppm": {
        "low": 0.017,  # 50% of limit
        "moderate": 0.035,  # Approx 24h limit
        "high": 0.075,  # High concentration
    },
    "co_ppm": {
        "low": 2.2,  # 50% of limit
        "moderate": 4.4,  # Approx 24h limit
        "high": 12.0,  # High concentration
    },
}

POLLUTANT_NAMES = {
    "pm25_ugm3": "PM2.5 µg/m³",
    "pm10_ugm3": "PM10 µg/m³",
    "o3_ppm": "O3 ppm",
    "no2_ppm": "NO2 ppm",
    "so2_ppm": "SO2 ppm",
    "co_ppm": "CO ppm",
}
POLLUTANT_LABELS = {
    "pm25_ugm3": "Particules fines (PM2.5)",
    "pm10_ugm3": "Particules fines (PM10)",
    "o3_ppm": "Ozone (O3)",
    "no2_ppm": "Dioxyde d'azote (NO2)",
    "so2_ppm": "Dioxyde de soufre (SO2)",
    "co_ppm": "Monoxyde de carbone (CO)",
}


class SeoulDashboard:
    def __init__(self):
        self.seoul_map = SeoulMap()

        self.loaded_days = pd.DataFrame()

    def get_loaded_days(self) -> pd.DataFrame:
        # # BigQuery
        # query = """
        #     SELECT
        #         *
        #     FROM `openaq-anomaly-prediction.dev_serving.ml_seoul__xgb_features`
        #     -- FROM `openaq-anomaly-prediction.dev_intermediate.int_seoul__measurements_segments`
        #     WHERE datetimeto_utc >= '2025-11-01'
        #     AND datetimeto_utc <= '2025-12-31'
        #     --  AND sensor_name = 'pm25 µg/m³'
        #     --    AND segment_total_hours >= 504
        #     ORDER BY location_id, datetimeto_utc ASC
        # """
        # query_parameters = [
        #     # ("test_locations_ids", "STRING", TEST_LOCATIONS)
        # ]
        # export_df = bq.query(query, query_parameters=query_parameters, dry_run=True, debug=True)

        if self.loaded_days.empty:
            # From parquet files
            output_path = os.path.join(
                cfg.DATA_EXPORT_PATH, "all_best_predictions_v1.parquet"
            )

            con = duckdb.connect()
            query = f"""
                SELECT
                    *
                FROM read_parquet('{output_path}')
                WHERE oaq_datetimeto_local >= '2025-11-01'
            """
            custom_df = con.execute(query, {}).df()
            self.loaded_days = custom_df
            # self.loaded_days = pd.read_parquet(output_path)

        return self.loaded_days

    def get_daily_data(self, date_filter: str) -> pd.DataFrame:
        # Get the loaded days dataframe or load it if not already done
        loaded_days = self.get_loaded_days()

        current_date = pd.to_datetime(date_filter).date()

        # # D-1 to D+2 range
        # start_date = current_date - timedelta(days=1)
        # end_date = start_date + timedelta(days=2)
        # D to D+2 range
        start_date = current_date
        end_date = start_date + timedelta(days=2)

        # Filter the right day and interpolate missing pollutant values
        daily_df = loaded_days[
            (loaded_days["oaq_datetimeto_local"].dt.date >= start_date)
            & (loaded_days["oaq_datetimeto_local"].dt.date <= end_date)
        ].copy()
        daily_df = CustomXGBoostModel().interpolate_pollutants(daily_df, debug=False)

        # Add the 'hour_of_day' column
        daily_df["local_hour"] = daily_df["oaq_datetimeto_local"].dt.hour
        daily_df.loc[:, "oaq_datetimeto_local_str"] = daily_df[
            "oaq_datetimeto_local"
        ].dt.strftime("%Y-%m-%d %H:%M")

        return daily_df

    def get_daily_pollutant_chart(
        self, daily_average_df: pd.DataFrame, pollutant_col: str
    ) -> PlotlyFigure:
        """Generate a line chart for daily average pollutant levels."""

        current_threshold = WHO_THRESHOLDS[pollutant_col]["moderate"]

        # Calculate a Y position roughly 80% up the chart (similar to your y=0.8 paper ref)
        y_min = min(
            daily_average_df[pollutant_col].min(),
            daily_average_df[pollutant_col + "_pred"].min(),
        )
        y_max = max(
            daily_average_df[pollutant_col].max(),
            daily_average_df[pollutant_col + "_pred"].max(),
        )
        text_y_pos = y_min + (y_max - y_min) * 0.8

        # 2. Initialize Empty Figure
        fig = go.Figure()

        # --- NEW SECTION: Date Separators & Labels ---
        # Get unique dates from the data to place lines/labels dynamically
        unique_dates = sorted(daily_average_df["oaq_datetimeto_local"].dt.date.unique())

        for i, date_val in enumerate(unique_dates):
            # A. Add Date Label (Centered at Noon)

            noon_ts = pd.Timestamp(date_val) + timedelta(hours=12)
            if i == 0:
                noon_ts -= timedelta(minutes=160)
            if i == 2:
                noon_ts += timedelta(minutes=160)

            font_size = 32 if i == 0 else 24 if i == 1 else 24
            font_color = (
                "rgba(120, 120, 120, 0.4)" if i == 1 else "rgba(120, 120, 120, 0.4)"
            )
            fig.add_trace(
                go.Scatter(
                    x=[noon_ts],  # Must be a list
                    y=[text_y_pos],  # Must be a list
                    mode="text",
                    text=[date_val.strftime("%d/%m")],
                    textfont=dict(
                        size=font_size,
                        color=font_color,
                        family="Google Sans, sans-serif",
                        weight="bold",
                    ),
                    hoverinfo="skip",  # Don't show tooltip for the date label
                    showlegend=False,  # Don't show in legend
                )
            )

            # B. Add Vertical Separator (At Midnight)
            # We skip index 0 because we don't need a line at the very start
            if i > 0:  # Assuming hourly data, lines at 24h intervals}:
                if i == 1:
                    midnight_ts = pd.Timestamp(date_val) - timedelta(minutes=120)

                if i == 2:
                    midnight_ts = pd.Timestamp(date_val) + timedelta(minutes=120)

                fig.add_vline(
                    x=midnight_ts,
                    line_width=1,
                    line_dash="dot",
                    line_color="rgba(200, 200, 200, 0.5)",  # Light gray separator
                    layer="below",
                )

        # 1. BACKGROUND LINE (Grey) - Drawn First
        fig.add_trace(
            go.Scatter(
                x=daily_average_df["oaq_datetimeto_local"],
                y=daily_average_df[pollutant_col + "_pred"],
                mode="lines",
                name="Prédictions",
                line=dict(
                    color="rgba(205, 46, 58, 1)",
                    # color="rgba(255, 209, 103, 1)",
                    # color="rgba(0, 71, 160, 1)",
                    width=2,
                    dash="dot",
                ),
            )
        )
        # We use px.line here to initialize the figure and axes
        # fig = px.line(
        #     daily_average_df,
        #     x="oaq_datetimeto_local",
        #     y=pollutant_col + "_pred",
        #     color_discrete_sequence=["rgba(205, 46, 58, 0.6)"],
        #     # color_discrete_sequence=["rgba(255, 209, 103, 0.6)"],
        #     # color_discrete_sequence=["rgba(0, 71, 160, 0.6)"],
        #     labels={"oaq_datetimeto_local": "Time (Local)"},
        #     range_y=[min_y_range * 0.9, max_y_range * 1.1],
        # )
        # # Configure the Grey Line (Background)
        # fig.update_traces(
        #     name="Mesures réelles", showlegend=True, line_width=3, line_dash="dot"
        # )

        # 2. FOREGROUND LINE (Yellow) - Drawn Second (On Top)

        fig.add_trace(
            go.Scatter(
                x=daily_average_df["oaq_datetimeto_local"],
                y=daily_average_df[pollutant_col],
                mode="lines",
                name="Mesures réelles",
                line=dict(
                    # color="rgba(205, 46, 58, 1)",
                    color="rgba(255, 209, 103, 1)",
                    # color="rgba(0, 71, 160, 1)",
                    width=4,
                    dash="solid",
                ),
            )
        )

        # WHO Threshold line
        fig.add_hline(
            y=current_threshold,
            line_dash="dash",  # Options: "solid", "dot", "dash", "longdash", "dashdot"
            line_color="rgba(120,120,120, 0.4)",  # Optional: Change color
            line_width=2,  # Optional: Change thickness
            annotation_text="OMS",  # Optional: Add a text label
            annotation_position="top left",  # Optional: Position the label
            annotation_font=dict(size=12, color="gray"),
            # layer="below",
        )

        # The axes legends
        fig.update_xaxes(
            title="Heure locale (GMT+9)",
            title_font=dict(size=12),
            tickformat="%H:%M",  # Format as Hour:Minute
            dtick=3600000 * 3,  # 3 hours in milliseconds
            tickfont=dict(size=12, color="gray"),
            # To strictly ensure the last tick (23:00) is shown:
            # We force the range to start/end exactly on your data limits
            range=[
                # daily_average_df["oaq_datetimeto_local"].min(),
                # daily_average_df["oaq_datetimeto_local"].max(),
                daily_average_df["oaq_datetimeto_local"].min() - timedelta(minutes=180),
                daily_average_df["oaq_datetimeto_local"].max() + timedelta(minutes=180),
            ],
        )
        fig.update_yaxes(
            showticklabels=True,  # Hide the numbers on the Y-axis
            # title=POLLUTANT_NAMES[pollutant_col],
            title=None,
            title_font=dict(size=12),
            # Use ticks to add margin
            ticks="outside",
            tickfont=dict(size=12, color="gray"),
            ticklen=8,
            tickcolor="rgba(211, 211, 211, 0)",
            range=[y_min * 0.8, y_max * 1.2],
        )

        # ---------------------------------------------------------
        # LAYER 4: ANOMALY MARKERS (On Top)
        # ---------------------------------------------------------
        # Filter data: Keep only rows where is_anomaly is True (or 1)
        anomalies_df = daily_average_df[
            daily_average_df["is_anomaly"] == True
        ]  # Ensure boolean or 1/0

        if not anomalies_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=anomalies_df["oaq_datetimeto_local"],
                    y=anomalies_df[
                        pollutant_col
                    ],  # Place marker on the YELLOW line (Prediction)
                    # y=anomalies_df[pollutant_col + "_pred"], # OR use this if you want it on the GREY line
                    mode="markers",
                    name="Anomalie",
                    marker=dict(
                        color="red",  # Red warning color
                        size=16,  # Big enough to see
                        symbol="x",  # circle, x, star-diamond
                        line=dict(
                            width=3, color="white"
                        ),  # White border to pop against the line
                    ),
                    hoverinfo="x+y+name",  # Show "Anomalie" on hover
                )
            )

        # The grid
        fig.update_layout(
            title=dict(
                text=f"<b>{POLLUTANT_NAMES[pollutant_col]}</b>",  # HTML tags work here
                x=0.0,  # 0 is far left, 1 is far right
                y=0.96,  # 1 is very top, 0 is very bottom
                xanchor="left",  # Anchor the text's left side to the x-coordinate
                yanchor="top",  # Anchor the text's top side to the y-coordinate
                font=dict(size=20, family="Google Sans, sans-serif"),
            ),
            height=400,
            margin=dict(l=0, t=36, r=0, b=0),
            plot_bgcolor="white",  # Clean background color
            font=dict(size=16, family="Google Sans, sans-serif"),  # Consistent font
            dragmode=False,
            showlegend=True,  # Ensure legend is on so we see "Prediction"
            legend=dict(
                orientation="h",  # Horizontal legend at bottom/top
                yanchor="bottom",
                y=1.06,
                xanchor="right",
                x=1,
            ),
            xaxis=dict(
                showgrid=False,
                gridcolor="rgba(200, 200, 200, 0.5)",  # Light gray grid
                gridwidth=1,
                fixedrange=True,
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(200, 200, 200, 0.5)",
                gridwidth=1,
                griddash="dot",  # solid, dot, dash, longdash, dashdot
                fixedrange=True,
            ),
        )

        return fig

    def get_daily_map(self, daily_df: pd.DataFrame, pollutant_col: str) -> PlotlyFigure:
        # pollutant_col = "pm25_ugm3"
        # pollutant_col = "o3_ppm"

        current_threshold = WHO_THRESHOLDS[pollutant_col]

        smoothed_df = self.seoul_map.smooth_dataframe_for_animation(
            daily_df,
            date_col="oaq_datetimeto_local",
            location_col="location_id",
            lat_col="oaq_latitude",
            lon_col="oaq_longitude",
            value_cols=[pollutant_col],
            freq="5min",  # 15 minutes = 4x more frames. Use '5min' for ultra-smooth.
        )

        fig = self.seoul_map.scatter_map(
            smoothed_df,
            lat_column="oaq_latitude",
            lon_column="oaq_longitude",
            title="SEOUL - Scatter Map",
            label=f"{POLLUTANT_NAMES[pollutant_col]}",
            animation_frame="animation_step",
            animation_group="location_id",
            color_column=pollutant_col,
            size_column=pollutant_col,
            size_max=64,
            exponential=True,
            coloraxes_min=smoothed_df[pollutant_col].min(),
            coloraxes_max=smoothed_df[pollutant_col].max(),
            # coloraxes_min=0,
            # coloraxes_max=current_threshold,
            map_style="basic",
            # map_style="satellite",
            hover_name="location_id",
            hover_data={
                "location_id": True,
                # "location_municipality": True,
                # "nan_pct": ":.2%",
                "oaq_latitude": False,
                "oaq_longitude": False,
                pollutant_col: ":.2f",
                # "location_perfect_hours": True,
                # "location_type": True,
            },
            map_layers=True,
            map_layers_glowing_border=False,
            map_layers_region="seoul",
        )

        return fig
