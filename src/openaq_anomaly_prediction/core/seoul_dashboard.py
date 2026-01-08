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
    "pm25_ugm3": 15,  # µg/m³
    "pm10_ugm3": 45,  # µg/m³
    "o3_ppm": 0.064,  # ppm
    "no2_ppm": 0.053,  # ppm
    "so2_ppm": 0.035,  # ppm
    "co_ppm": 4.4,  # ppm
}

POLLUTANT_NAMES = {
    "pm25_ugm3": "PM2.5 µg/m³",  # µg/m³
    "pm10_ugm3": "PM10 µg/m³",  # µg/m³
    "o3_ppm": "O3 ppm",  # ppm
    "no2_ppm": "NO2 ppm",  # ppm
    "so2_ppm": "SO2 ppm",  # ppm
    "co_ppm": "CO ppm",  # ppm
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
                cfg.DATA_EXPORT_PATH, "xgb_features_total.parquet"
            )

            con = duckdb.connect()
            query = f"""
                SELECT
                    *
                FROM read_parquet('{output_path}')
                WHERE datetimeto_utc >= '2025-11-01'
            """
            custom_df = con.execute(query, {}).df()
            self.loaded_days = custom_df
            # self.loaded_days = pd.read_parquet(output_path)

        return self.loaded_days

    def get_daily_data(self, date_filter: str) -> pd.DataFrame:
        # Get the loaded days dataframe or load it if not already done
        loaded_days = self.get_loaded_days()

        # Filter the right day and interpolate missing pollutant values
        daily_df = loaded_days[
            loaded_days["oaq_datetimeto_local"].dt.date
            == pd.to_datetime(date_filter).date()
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

        current_threshold = WHO_THRESHOLDS[pollutant_col]
        # max_y_range = (
        #     daily_average_df[pollutant_col].max()
        #     if daily_average_df[pollutant_col].max() > current_threshold
        #     else current_threshold
        # )

        # The plotly figure
        fig = px.line(
            daily_average_df,
            x="oaq_datetimeto_local",
            y=pollutant_col,
            color_discrete_sequence=["rgba(255, 209, 103, 0.8)"],  # Custom line color
            # text=pollutant_col,
            labels={
                "oaq_datetimeto_local": "Time (Local)",
                # pollutant_col: f"{pollutant_col} Levels",
            },
            range_y=[
                daily_average_df[pollutant_col].min() * 0.8,
                daily_average_df[pollutant_col].max() * 1.2,
            ],
        )
        fig.add_hline(
            y=current_threshold,
            line_dash="dash",  # Options: "solid", "dot", "dash", "longdash", "dashdot"
            line_color="rgba(120,120,120, 0.4)",  # Optional: Change color
            line_width=2,  # Optional: Change thickness
            annotation_text="OMS",  # Optional: Add a text label
            annotation_position="top left",  # Optional: Position the label
            annotation_font=dict(size=12, color="gray"),
        )

        fig.update_traces(line_width=4)  # Default is usually 2

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
                daily_average_df["oaq_datetimeto_local"].min() - timedelta(minutes=90),
                daily_average_df["oaq_datetimeto_local"].max() + timedelta(minutes=90),
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
        )

        # The grid
        fig.update_layout(
            title=dict(
                text=f"<b>{POLLUTANT_NAMES[pollutant_col]}</b>",  # HTML tags work here
                x=0.0,  # 0 is far left, 1 is far right
                y=0.98,  # 1 is very top, 0 is very bottom
                xanchor="left",  # Anchor the text's left side to the x-coordinate
                yanchor="top",  # Anchor the text's top side to the y-coordinate
                font=dict(size=20, family="Google Sans, sans-serif"),
            ),
            height=320,
            margin=dict(l=0, t=36, r=0, b=0),
            plot_bgcolor="white",  # Clean background color
            font=dict(size=16, family="Google Sans, sans-serif"),  # Consistent font
            dragmode=False,
            xaxis=dict(
                showgrid=False,
                gridcolor="rgba(211, 211, 211, 0.6)",  # Light gray grid
                gridwidth=1,
                fixedrange=True,
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(211, 211, 211, 0.6)",
                gridwidth=1,
                griddash="dot",  # solid, dot, dash, longdash, dashdot
                fixedrange=True,
            ),
        )

        # # 2. THE EXTRA LABEL (Upper Top Right)
        # fig.add_annotation(
        #     text="Last updated: 10m ago",
        #     xref="paper",
        #     yref="paper",  # "paper" means coordinates are 0-1 relative to the plot area
        #     x=1,
        #     y=1,  # Top Right corner
        #     showarrow=False,
        #     xanchor="right",  # Anchor right side of text to x=1
        #     yanchor="top",
        #     font=dict(size=12, color="gray"),
        # )

        # # ADD COLOR SCALED MARKERS
        # fig.add_trace(
        #     go.Scatter(
        #         x=daily_average_df["oaq_datetimeto_local"],
        #         y=daily_average_df[pollutant_col],
        #         mode="markers",  # Markers only
        #         marker=dict(
        #             color=daily_average_df[pollutant_col],  # Map color to Value
        #             colorscale="RdYlGn_r",  # Red-Yellow-Green (Reversed so Red is high)
        #             size=16,  # Adjust size
        #             showscale=False,  # Hides the extra colorbar if you don't need it
        #         ),
        #         name="Value Intensity",
        #         hoverinfo="skip",  # Avoid double tooltips
        #     )
        # )

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
