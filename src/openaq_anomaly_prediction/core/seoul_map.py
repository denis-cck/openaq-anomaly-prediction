import copy
from pprint import pprint
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import seaborn as sns
from plotly.graph_objs import Figure as PlotlyFigure

from openaq_anomaly_prediction.core.base_map import BaseMap

# ================================================================
# MAPS COLOR SCALES

KOREAN_FLAG_CS = [
    # # TIER 1: Deep Ink (0% - 25%)
    # [0.0, "rgba(20, 20, 20, 0.1)"],
    # [0.25, "rgba(20, 20, 20, 0.2)"],
    # TIER 1.5: Deep Ink (0% - 25%)
    [0.0, "rgba(60, 60, 60, 0.4)"],
    [0.25, "rgba(60, 60, 60, 0.5)"],
    # # TIER 2: Official Blue (25% - 50%)
    # [0.25, "rgba(0, 71, 160, 0.6)"],
    # [0.5, "rgba(0, 71, 160, 0.7)"],
    # TIER 2.5: Official Blue (25% - 50%)
    [0.25, "rgba(0, 71, 160, 0.5)"],
    [0.5, "rgba(0, 71, 160, 0.6)"],
    # TIER 3: Royal Yellow (50% - 75%)
    [0.5, "rgba(255, 209, 103, 0.8)"],
    [0.75, "rgba(255, 209, 103, 0.8)"],
    # TIER 4: Cinnabar Red (75% - 100%)
    [0.75, "rgba(205, 46, 58, 0.9)"],
    [1.0, "rgba(205, 46, 58, 0.9)"],
]

WHITE_TRANSPARENT_CS = [
    # TIER 1: Invisible/Low (0% - 20%)
    [0.0, "rgba(255, 255, 255, 0)"],  # Fully transparent
    [0.2, "rgba(255, 255, 255, 0.2)"],  # Faint white "ghost" points
    # # TIER 1.5: KEEP VISIBLE (20% - 50%)
    # [0.0, "rgba(0, 0, 0, 1)"],  # Fully transparent
    # [0.2, "rgba(0, 0, 0, 1)"],  # Faint white "ghost" points
    # TIER 2: Cool/Stable (20% - 50%)
    [0.2, "rgba(255, 255, 255, 0.5)"],  # Start Official Blue (50% Opacity)
    [0.5, "rgba(255, 255, 255, 0.6)"],
    # TIER 3: Warning/Center (50% - 80%)
    [0.5, "rgba(255, 255, 255, 0.9)"],  # Start Royal Yellow (90% Opacity)
    [0.8, "rgba(255, 255, 255, 0.9)"],
    # TIER 4: Critical/Peak (80% - 100%)
    [0.8, "rgba(255, 255, 255, 0.9)"],  # Start Cinnabar Red (Fully Opaque)
    [1.0, "rgba(255, 255, 255, 0.9)"],
]

# ---------------------------------------------------------------------

KOREAN_FLAG_CS_SWAP = [
    # TIER 1: Deep Ink (0% - 25%)
    [0.0, "rgba(20, 20, 20, 0.1)"],
    [0.25, "rgba(20, 20, 20, 0.2)"],
    # TIER 3: Royal Yellow (50% - 75%)
    [0.25, "rgba(255, 209, 103, 0.6)"],
    [0.5, "rgba(255, 209, 103, 0.7)"],
    # TIER 2: Official Blue (25% - 50%)
    [0.5, "rgba(0, 71, 160, 0.8)"],
    [0.75, "rgba(0, 71, 160, 0.8)"],
    # TIER 4: Cinnabar Red (75% - 100%)
    [0.75, "rgba(205, 46, 58, 0.8)"],
    [1.0, "rgba(205, 46, 58, 0.8)"],
]

KOREAN_FLAG_CS_GOLD = [
    # TIER 1: Deep Ink (0% - 25%)
    [0.0, "rgba(20, 20, 20, 0.1)"],
    [0.25, "rgba(20, 20, 20, 0.2)"],
    # TIER 2: Official Blue (25% - 50%)
    [0.25, "rgba(0, 71, 160, 0.6)"],
    [0.5, "rgba(0, 71, 160, 0.7)"],
    # TIER 3: Royal Yellow (50% - 75%)
    [0.5, "rgba(205, 46, 58, 0.8)"],
    [0.75, "rgba(205, 46, 58, 0.8)"],
    # TIER 4: Cinnabar Red (75% - 100%)
    [0.75, "rgba(255, 209, 103, 0.8)"],
    [1.0, "rgba(255, 209, 103, 0.8)"],
]

KOREAN_FLAG_LIGHT_CS = [
    # TIER 1: Deep Ink (0% - 25%)
    [0.0, "rgba(255, 255, 255, 0.4)"],
    [0.25, "rgba(255, 255, 255, 0.5)"],
    # TIER 1: Deep Ink (0% - 25%)
    [0.0, "rgba(255, 255, 255, 0.4)"],
    [0.25, "rgba(255, 255, 255, 0.5)"],
    # TIER 2: Official Blue (25% - 50%)
    [0.25, "rgba(0, 71, 160, 0.6)"],
    [0.5, "rgba(0, 71, 160, 0.7)"],
    # TIER 3: Royal Yellow (50% - 75%)
    [0.5, "rgba(255, 209, 103, 0.8)"],
    [0.75, "rgba(255, 209, 103, 0.8)"],
    # TIER 4: Cinnabar Red (75% - 100%)
    [0.75, "rgba(205, 46, 58, 0.8)"],
    [1.0, "rgba(205, 46, 58, 0.8)"],
]

# ---------------------------------------------------------------------

AQI_PALETTE = [
    [0.0, "rgba(0, 228, 0, 0)"],  # Clean/Zero: Transparent Green
    [0.2, "rgba(255, 255, 0, 0.4)"],  # Moderate: Translucent Yellow
    [0.5, "rgba(255, 126, 0, 0.7)"],  # Unhealthy: Orange
    [0.8, "rgba(255, 0, 0, 0.9)"],  # Very Unhealthy: Red
    [1.0, "rgba(143, 63, 151, 1)"],  # Hazardous: Solid Purple
]

TOXIC_PALETTE = [
    [0.0, "rgba(0, 255, 255, 0)"],  # Baseline: Transparent Cyan
    [0.3, "rgba(173, 255, 47, 0.3)"],  # Low: Green-Yellow
    [0.6, "rgba(255, 255, 0, 0.6)"],  # Mid: Bright Yellow
    [1.0, "rgba(255, 0, 255, 1)"],  # Peak: Solid Magenta/Pink
]

THERMAL_PALETTE = [
    [0.0, "rgba(255, 245, 240, 0)"],  # Start: Transparent White
    [0.2, "rgba(254, 224, 210, 0.3)"],  # Light Peach
    [0.5, "rgba(252, 146, 114, 0.6)"],  # Soft Coral
    [0.8, "rgba(222, 45, 38, 0.8)"],  # Strong Red
    [1.0, "rgba(103, 0, 13, 1)"],  # Peak: Deep Maroon
]


class SeoulMap(BaseMap):
    """Seoul Map Class."""

    def __init__(self):
        super().__init__()

        self.seoul_geojson: dict[Any, Any] = {}
        self.seoul_geojson_districts: dict[Any, Any] = {}

        self.korea_geojson: dict[Any, Any] = {}
        self.neighbours_geojson_districts: dict[Any, Any] = {}

        self.preload_map()

    def preload_map(self):
        """Preload Seoul map data."""
        # self.map_name = "Seoul"
        # self.map_file = "seoul_map.geojson"
        # self.map_crs = "EPSG:5179"  # Korean 2000 / Unified CS
        # self.center_coordinates = [37.5665, 126.9780]  # Seoul City Center
        # self.zoom_start = 11

        # ================================================================
        # MAPS PRELOAD (GEOJSON)

        # REGIONS
        if len(self.seoul_geojson) == 0:
            seoul_geojson_url = "https://raw.githubusercontent.com/southkorea/southkorea-maps/master/kostat/2018/json/skorea-provinces-2018-geo.json"
            seoul_geojson_features = requests.get(seoul_geojson_url).json()["features"]

            # Seoul
            self.seoul_geojson = {
                "type": "FeatureCollection",
                "features": [
                    f
                    for f in seoul_geojson_features
                    if f["properties"].get("name_eng") == "Seoul"
                    or f["properties"].get("name") == "서울특별시"
                ],
            }

            # Seoul's Neighbors (Gyeonggi-do + Incheon)
            target_regions = {
                "Seoul",
                "서울특별시",
                "Gyeonggi-do",
                "경기도",
                "Incheon",
                "인천광역시",
            }
            self.korea_geojson = {
                "type": "FeatureCollection",
                "features": [
                    f
                    for f in seoul_geojson_features
                    if f["properties"].get("name") in target_regions
                    or f["properties"].get("name_eng") in target_regions
                ],
            }

        # MUNICIPALITIES
        if len(self.seoul_geojson_districts) == 0:
            # seoul_geojson_districts_url = "https://raw.githubusercontent.com/southkorea/seoul-maps/master/kostat/2013/json/seoul_municipalities_geo_simple.json"
            seoul_geojson_districts_url = "https://raw.githubusercontent.com/southkorea/seoul-maps/refs/heads/master/juso/2015/json/seoul_municipalities_geo_simple.json"
            self.seoul_geojson_districts = requests.get(
                seoul_geojson_districts_url
            ).json()

        # SEOUL Municipalities + Neighbors (SIG Level)
        # We check if empty OR force reload to ensure neighbors are added
        if len(self.neighbours_geojson_districts) == 0:
            # Use the nationwide map you verified
            korea_municipalities_url = "https://raw.githubusercontent.com/southkorea/southkorea-maps/master/kostat/2018/json/skorea-municipalities-2018-geo.json"

            # List of neighbors (Root names only)
            target_neighbors = [
                "Gyeyang",
                "Bupyeong",
                "Namdong",
                "Gimpo",
                "Goyang",
                "Yangju",
                "Uijeongbu",
                "Namyangju",
                "Guri",
                "Hanam",
                "Seongnam",
                "Gwangju",
                "Gwacheon",
                "Anyang",
                "Gwangmyeong",
                "Bucheon",
                "Siheung",
                "Uiwang",
                "Gunpo",
            ]

            resp = requests.get(korea_municipalities_url)

            if resp.status_code == 200:
                full_map = resp.json()
                filtered_features = []

                for f in full_map["features"]:
                    props = f["properties"]

                    # 1. GET CORRECT KEYS
                    name_kr = props.get("name", "")
                    name_eng = props.get("name_eng", "")  # Verified key
                    code = str(props.get("code", ""))

                    # 2. FILTER LOGIC

                    # # A. Is it Seoul? (Codes starting with 11)
                    # is_seoul = code.startswith("11")

                    # B. Is it a Neighbor? (Partial Match)
                    # Checks if "Goyang" is inside "Goyangsideogyanggu"
                    is_neighbor = any(city in name_eng for city in target_neighbors)

                    if is_neighbor:
                        filtered_features.append(f)

                self.neighbours_geojson_districts = {
                    "type": "FeatureCollection",
                    "features": filtered_features,
                }

            else:
                print(f"❌ Map Download Failed: {resp.status_code}")

    def scatter_map(
        self, df: pd.DataFrame, lat_column: str, lon_column: str, *args, **kwargs
    ):
        """Scatter plot on Seoul map."""

        map_df = df.copy()

        map_style = kwargs.get("map_style", "basic")

        title = kwargs.get("title", "SEOUL - Scatter Map")
        title_config = kwargs.get("title_config", {"upper": False})

        label = kwargs.get("label", "Pollutant")
        label_config = kwargs.get("label_config", {})

        colorbar_config = kwargs.get(
            "colorbar_config",
            {
                "option": "horizontal",
                # "option": "vertical",
            },
        )

        color_column = kwargs.get("color_column", None)
        size_column = kwargs.get("size_column", None)
        size_max = kwargs.get("size_max", 48)

        hover_name = kwargs.get("hover_name", None)
        hover_data = kwargs.get("hover_data", None)

        exponential = kwargs.get("exponential", None)

        coloraxes_min = kwargs.get("coloraxes_min", map_df[color_column].min())
        coloraxes_max = kwargs.get("coloraxes_max", map_df[color_column].max())

        map_layers = kwargs.get("map_layers", True)
        map_layers_glowing_border = kwargs.get("map_layers_glowing_border", False)

        # EXPONENTIAL MODE
        if exponential and size_column is not None:
            # Example: Square the values to create exponential visual scaling
            # clip = coloraxes_max if coloraxes_max is not None else 80
            # clip = 100
            # map_df[size_column + "_exp"] = map_df[size_column].clip(upper=clip) ** 2
            map_df[size_column + "_exp"] = map_df[size_column] ** 2
            # map_df[size_column + "_exp"] = map_df[size_column]  # disable
            size_column = size_column + "_exp"
            if hover_data is not None:
                hover_data[size_column] = False

            # Cap the max size for the visual scaling to avoid excessively large scales
            # map_df.loc[map_df[size_column] > 80, size_column] = 80

        # if exponential and color_column is not None:
        #     # Example: Square the values to create exponential visual scaling
        #     map_df[color_column + "_exp"] = map_df[color_column] ** 3
        #     # map_df[size_column + "_exp"] = map_df[size_column]
        #     color_column = color_column + "_exp"
        #     if hover_data is not None:
        #         hover_data[color_column] = False

        #     coloraxes_min = map_df[color_column].min()
        #     coloraxes_max = map_df[color_column].max()

        coloraxes_config = kwargs.get(
            "coloraxes_config",
            {
                "point_coloraxis": KOREAN_FLAG_CS,
                "border_coloraxis": WHITE_TRANSPARENT_CS,
                "cmin": coloraxes_min,
                "cmax": coloraxes_max,
            },
        )

        animation_frame = kwargs.get("animation_frame", None)
        animation_group = kwargs.get("animation_group", None)

        # ---------------------------------------------------------------------
        # SORT THE DATAFRAME FOR ANIMATION LAYERS

        sort_keys = []
        if animation_frame and animation_frame in map_df.columns:
            sort_keys.append(animation_frame)

        if size_column and size_column in map_df.columns:
            sort_keys.append(size_column)

        if sort_keys:
            # ascending=True puts Smallest first (bottom layer) and Biggest last (top layer)
            map_df = map_df.sort_values(by=sort_keys, ascending=True)

        # ---------------------------------------------------------------------
        # CREATE THE FIGURE

        fig = px.scatter_map(
            map_df,
            lat=lat_column,
            lon=lon_column,
            color=color_column,
            size=size_column,
            size_max=size_max,
            hover_name=hover_name,
            hover_data={animation_frame: False, **hover_data},  # type: ignore
            animation_frame=animation_frame,
            animation_group=animation_group,
            opacity=0.9,  # Add this here
            range_color=[coloraxes_min, coloraxes_max],
        )

        # ---------------------------------------------------------------------
        # ANNOTATIONS (ANIMATION UPDATES)

        annotations = []
        # annotations += self.customize_title(title, **title_config)

        first_datetime = df["oaq_datetimeto_local"].iloc[0]

        # days_fr = [
        #     "Lundi",
        #     "Mardi",
        #     "Mercredi",
        #     "Jeudi",
        #     "Vendredi",
        #     "Samedi",
        #     "Dimanche",
        # ]
        # current_day_fr = days_fr[first_datetime.weekday()]
        # date_str = f"{current_day_fr} {first_datetime.strftime('%d/%m')}"

        initial_hour_anno = self.customize_hour(first_datetime.strftime("%H:%M"))
        # static_date_anno = self.customize_date(first_datetime.strftime("%d/%m/%Y"))
        # static_date_anno = self.customize_date(
        #     f'<span style="font-size: 48px;">{current_day_fr}</span><br><span style="font-size: 24px;">{first_datetime.strftime("%d/%m/%Y")}</span>'
        # )
        static_label_anno = self.customize_label(label, **label_config)

        annotations = initial_hour_anno + static_label_anno
        # annotations = initial_hour_anno + static_date_anno + static_label_anno

        # ---------------------------------------------------------------------
        # Update each frame's layout with the updated map layers and title

        # Initialize each base annotation
        base_annotations_list = static_label_anno

        # 2. Loop through every frame to update the text
        for frame in fig.frames:
            # frame.name holds the value of the animation_frame column (e.g., "2025-12-11 14:30")
            frame_value = frame.name  # type: ignore

            dt_current = pd.to_datetime(frame_value)
            new_hour_str = dt_current.strftime("%H:%M")

            dynamic_hour_anno = self.customize_hour(new_hour_str)
            frame_annotations = dynamic_hour_anno + base_annotations_list
            frame.layout.annotations = frame_annotations

        # ---------------------------------------------------------------------
        # SLIDER

        # Vertical position for the whole row
        CONTROL_ROW_Y = 0.086
        BUTTONS_ROW_Y = CONTROL_ROW_Y + 0.019

        # Horizontal spacing logic
        SLIDER_X_START = 0.16  # Where the slider track begins
        BUTTON_GAP = 0.0  # Gap between buttons
        SLIDER_GAP = 0.022  # Gap between Pause button and Slider

        # Calculated Button Positions (Right to Left)
        PAUSE_BTN_X = SLIDER_X_START - SLIDER_GAP
        PLAY_BTN_X = PAUSE_BTN_X - BUTTON_GAP

        # Customize the slider
        fig.layout.sliders[0].update(
            active=0,
            yanchor="top",
            xanchor="left",
            # 1. Position and Length
            currentvalue={
                "prefix": "",  # Text before the timestamp
                "visible": False,
                "xanchor": "left",
                "font": {"size": 32, "color": "rgba(60, 60, 60, 0.0)"},
            },
            pad={"b": 0, "t": 0},
            len=0.52,  # Length of slider (0.9 = 90% of width)
            # Position logic
            x=SLIDER_X_START,  # Anchored X
            y=CONTROL_ROW_Y,  # Anchored Y
            # x=0.166,  # Horizontal start position
            # y=0.22,  # Vertical position
            # 2. Colors and Theme
            # bgcolor="#e8e8e8",  # Background of the track
            bgcolor="#FFFFFF",  # Background of the track
            activebgcolor="#FFFFFF",  # Color of the track "behind" the handle (progress)
            # Border
            bordercolor="rgba(128, 128, 128, 0.2)",  # border of the container
            # bordercolor="#FFFFFF",  # border of the container
            borderwidth=8,
            # Slider ticks and label (hiding both)
            ticklen=0,  # Hide tick lines if you have too many frames
            tickcolor="rgba(0,0,0,0)",
            font={"size": 14, "color": "rgba(0,0,0,0)"},
        )

        # Hide all the slider's labels
        for step in fig.layout.sliders[0].steps:
            original_label = step["label"]
            dt = pd.to_datetime(original_label)
            step["label"] = f"<b>{dt.strftime('%H:%M')}</b>"
            # step["label"] = ""

        # ---------------------------------------------------------------------
        # BUTTONS CONFIGURATION

        # What WORKS for sure: play(redraw=True, fromcurrent=True), pause(redraw=False)
        # We define the list separately
        custom_buttons = [
            {
                "type": "buttons",
                "showactive": False,
                "direction": "left",
                "x": PLAY_BTN_X,  # Calculated above
                "y": BUTTONS_ROW_Y,  # Same Y as slider
                # "x": 0.098,
                # "y": 0.102,
                "pad": {"r": 60, "t": 0},
                "font": {"color": "rgba(100, 100, 100, 0.8)"},
                "bgcolor": "white",
                "bordercolor": "rgba(128, 128, 128, 0.2)",
                "borderwidth": 8,
                "buttons": [
                    {
                        "label": "&#9654;",
                        "method": "animate",
                        "args": [
                            None,
                            {
                                "frame": {"duration": 100, "redraw": True},
                                "transition": {"duration": 0},
                                "fromcurrent": True,
                                "mode": "immediate",
                            },
                        ],
                    },
                ],
            },
            {
                "type": "buttons",
                "showactive": False,
                "direction": "left",
                # Position logic
                "x": PAUSE_BTN_X,  # Calculated above
                "y": BUTTONS_ROW_Y,  # Same Y as slider
                # "x": 0.146,
                # "y": 0.102,
                "pad": {"r": 10, "t": 0},
                "font": {"color": "rgba(100, 100, 100, 0.8)"},
                "bgcolor": "white",
                "bordercolor": "rgba(128, 128, 128, 0.2)",
                "borderwidth": 8,
                "buttons": [
                    {
                        "label": "&#10074;&#10074;",
                        "method": "animate",
                        "args": [
                            [None],
                            {
                                "frame": {"duration": 0, "redraw": False},
                                "mode": "immediate",
                                "transition": {"duration": 0},
                            },
                        ],
                    },
                ],
            },
        ]
        fig.layout.updatemenus = custom_buttons

        # ---------------------------------------------------------------------
        # MAP LAYERS

        map_layers_region = kwargs.get("map_layers_region", "seoul")
        if map_layers_region not in {"seoul", "korea"}:
            map_layers_region = "seoul"

        # Seoul
        if map_layers_region == "seoul":
            dimmer_color = "rgba(120, 120, 120, 0.5)"  # 0.4
            border_color = "rgba(255, 255, 255, 1)"
            sub_border_color = "rgba(180, 180, 180, 0.6)"
            # neighbours_geojson_districts = self.neighbours_geojson_districts
            neighbours_geojson_districts = None

        # Neighbors
        elif map_layers_region == "korea":
            dimmer_color = "rgba(0, 0, 0, 0.0)"
            border_color = "rgba(180, 180, 180, 1)"
            sub_border_color = "rgba(180, 180, 180, 1)"
            neighbours_geojson_districts = self.neighbours_geojson_districts
            # neighbours_geojson_districts = None
            map_layers_glowing_border = False  # Disable glowing for larger area

        neighbours_border_color = kwargs.get(
            "neighbours_border_color", "rgba(90, 90, 90, 1)"
        )

        if map_layers:
            fig.update_layout(
                map_layers=self.customize_map_layers(
                    city_geojson=self.seoul_geojson,
                    seoul_geojson_districts=self.seoul_geojson_districts,
                    neighbours_geojson_districts=neighbours_geojson_districts,
                    # neighbours_border_color=neighbours_border_color,
                    border_color=border_color,
                    border_width=5,
                    sub_border_color=sub_border_color,
                    sub_border_width=2,
                    sub_dashes=[3, 3],
                    dimmer_color=dimmer_color,
                    glowing_border=map_layers_glowing_border,
                ),
            )

        # ---------------------------------------------------------------------
        # CUSTOMIZE GENERAL LAYOUT

        fig.update_layout(
            width=1024,
            height=760,
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            map_style=map_style,  # ['basic', 'carto-darkmatter', 'carto-darkmatter-nolabels', 'carto-positron', 'carto-positron-nolabels', 'carto-voyager', 'carto-voyager-nolabels', 'dark', 'light', 'open-street-map', 'outdoors', 'satellite', 'satellite-streets', 'streets', 'white-bg']
            map_zoom=10.1,
            map_center={"lat": 37.55414276122411, "lon": 126.97022422324727},
            # Title
            annotations=annotations,
            # Color scales
            **self.customize_coloraxes(**coloraxes_config),
            # Legend
            coloraxis_colorbar=self.customize_colorbar(**colorbar_config),
        )
        return fig
