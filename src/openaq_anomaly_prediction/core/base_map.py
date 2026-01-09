from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import seaborn as sns
from plotly.graph_objs import Figure as PlotlyFigure

SEP_LENGTH = 64  # Only so we can print separators of a fixed length


class BaseMap:
    """Base class for map visualizations."""

    def __init__(self):
        pass

    # ================================================================
    # CUSTOMIZE COLORAXIES (COLOR SCHEMES) AND COLORBAR (LEGEND)

    def customize_colorbar(self, option: str = "horizontal", **kwargs) -> dict:
        """Customize colorbar for the map visualization."""
        if option == "vertical":
            x = kwargs.get("x", 0.926)
            y = kwargs.get("y", 0.815)
            len = kwargs.get("len", 0.3)
            thickness = kwargs.get("thickness", 15)
            bgcolor = kwargs.get("bgcolor", "rgba(255,255,255, 1)")

            return dict(
                # tickmode="array",
                # tickvals=[0, 10, 20, 30, 40, 50, 100],
                # ticktext=["0", "10", "20", "30", "40", "50", "100+"],
                x=x,
                y=y,
                len=len,
                thickness=thickness,
                bgcolor=bgcolor,
                bordercolor="white",
                borderwidth=1,
                title=dict(text=""),
                # tickfont=dict(size=12)
            )
        else:
            x = kwargs.get("x", 0.85)
            y = kwargs.get("y", 0.032)  # 0.06
            len = kwargs.get("len", 0.23)
            thickness = kwargs.get("thickness", 10)

            return dict(
                orientation="h",
                x=x,
                y=y,
                len=len,
                thickness=thickness,
                bgcolor="rgba(255,255,255, 1)",
                bordercolor="rgba(128, 128, 128, 0.2)",
                borderwidth=2,
                title=dict(text=""),
                # tickfont=dict(size=12)
            )

    def customize_coloraxes(self, **kwargs) -> dict:
        point_coloraxis = kwargs.get("point_coloraxis", None)
        border_coloraxis = kwargs.get("border_coloraxis", None)
        cmin = kwargs.get("cmin", 0)
        cmax = kwargs.get("cmax", None)

        if point_coloraxis is None or border_coloraxis is None:
            raise ValueError(
                "Both point_coloraxis and border_coloraxis must be provided."
            )

        return {
            # Point coloraxis
            "coloraxis": dict(
                colorscale=point_coloraxis,
                cmin=cmin,
                cmax=cmax,
                showscale=True,  # Ensure it's always visible
                # colorbar=customize_colorbar(option="vertical")
            ),
            # Border coloraxis
            "coloraxis2": dict(
                colorscale=border_coloraxis, cmin=cmin, cmax=cmax, showscale=False
            ),
        }

    # ================================================================
    # CUSTOMIZE MAP LAYERS (GEOJSON)

    def customize_map_layers(
        self,
        city_geojson: dict,
        seoul_geojson_districts: dict | None = None,
        neighbours_geojson_districts: dict | None = None,
        option: str = "default",
        **kwargs,
    ) -> list:
        # color = kwargs.get("color", "")
        dimmer_color = kwargs.get("dimmer_color", "rgba(0, 0, 0, 0.2)")
        glowing_border = kwargs.get("glowing_border", False)

        border_color = kwargs.get("border_color", "white")
        border_width = kwargs.get("border_width", 2)

        sub_border_color = kwargs.get("sub_border_color", "rgba(0,0,0,0.4)")
        sub_border_width = kwargs.get("sub_border_width", 2)
        sub_dashes = kwargs.get("sub_dashes", [2, 2])

        map_layers = []

        # Create the Mask geojson
        world_coords = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
        city_coords = city_geojson["features"][0]["geometry"]["coordinates"]
        mask_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [world_coords] + city_coords
                        if isinstance(city_coords[0][0], list)
                        else [world_coords, city_coords[0]],
                    },
                    "properties": {},
                }
            ],
        }

        if option == "default":
            map_layers = [
                # LAYER 1: The "Dimmer" (Everything EXCEPT Seoul)
                {
                    "sourcetype": "geojson",
                    "source": mask_geojson,
                    "type": "fill",
                    "color": f"{dimmer_color}",  # Darken the rest of the world
                    "below": "traces",
                },
                # LAYER 3: The Thick White Border for Seoul
                # {
                #     "sourcetype": "geojson",
                #     "source": city_geojson,
                #     "type": "line",
                #     "color": "rgba(255, 255, 255, 1)",
                #     "line": {"width": 2},
                #     "below": "traces"
                # },
                # # LAYER 4: Dim Seoul (less)
                # {
                #     "sourcetype": "geojson",
                #     "source": seoul_geojson,
                #     "type": "fill",
                #     "color": "rgba(0, 0, 0, 0.1)", # Darken Seoul
                #     "below": "traces"
                # },
            ]

            # LAYER: The Seoul Municipalities
            if seoul_geojson_districts is not None:
                map_layers.append(
                    {
                        "sourcetype": "geojson",
                        "source": seoul_geojson_districts,
                        "type": "line",
                        "color": f"{sub_border_color}",
                        "line": {
                            "width": sub_border_width,
                            "dash": sub_dashes,  # [dash length, gap length]
                        },
                        "below": "traces",
                    }
                )
            # LAYER: The Neighbouring Municipalities
            if neighbours_geojson_districts is not None:
                neighbours_border_color = kwargs.get(
                    "neighbours_border_color", sub_border_color
                )
                map_layers.append(
                    {
                        "sourcetype": "geojson",
                        "source": neighbours_geojson_districts,
                        "type": "line",
                        "color": f"{neighbours_border_color}",
                        "line": {
                            "width": sub_border_width,
                            "dash": sub_dashes,  # [dash length, gap length]
                        },
                        "below": "traces",
                    }
                )

            # BORDER: Seoul Borders
            if glowing_border:
                glow_layers = []
                for width, opacity in zip([2, 5, 10, 15], [0.8, 0.4, 0.2, 0.1]):
                    glow_layers.append(
                        {
                            "sourcetype": "geojson",
                            "source": city_geojson,
                            "type": "line",
                            "color": f"rgba(255, 255, 255, {opacity})",
                            "line": {"width": width},
                            "below": "traces",
                        }
                    )
                map_layers.extend(glow_layers)
            else:
                map_layers.extend(
                    [
                        {
                            "sourcetype": "geojson",
                            "source": city_geojson,
                            "type": "line",
                            "color": border_color,
                            "line": {"width": border_width},
                            "below": "traces",
                        }
                    ]
                )

        return map_layers

    # ================================================================
    # CUSTOMIZE POINTS

    # TODO: Add an exponential "size" feature for both the size of the points and the colors
    def customize_points(
        self, fig: PlotlyFigure, df: pd.DataFrame, lat: str, lon: str, **kwargs
    ) -> PlotlyFigure:
        color = kwargs.get("color", None)
        size_column = kwargs.get("size", None)
        size_max = kwargs.get("size_max", 24)
        border = kwargs.get("border", size_column is None)

        hover_name = kwargs.get("hover_name", None)
        hover_data = kwargs.get("hover_data", None)

        exponential = kwargs.get("exponential", False)

        # Reset all traces
        fig.data = []

        border_scaling_ratio = 1.5
        base_point_size = 16

        point_size = base_point_size if size_column is None else 0
        border_size = (
            int(point_size * border_scaling_ratio) if size_column is None else None
        )

        df_plot = df.copy()

        # EXPONENTIAL MODE
        if exponential and size_column:
            # Example: Square the values to create exponential visual scaling
            df_plot[size_column + "_exp"] = df_plot[size_column] ** 2
            size_column = size_column + "_exp"
            if hover_data is not None:
                hover_data[size_column] = False

        # FIXED SIZE (settings)
        if size_column is None:
            border_settings = {
                "coloraxis": "coloraxis2",
                "size": border_size if not size_column else None,
                "color": "white",
            }
            marker_settings = {
                "coloraxis": "coloraxis",
                "size": point_size,
            }
        # SCALED SIZE (settings)
        else:
            border_settings = {
                "coloraxis": "coloraxis2",
                # color="white",
                # opacity=0.8,
                # sizemode='area' if size else None,
                # sizeref=calc_sizeref if size else None,
            }
            marker_settings = {
                "coloraxis": "coloraxis",
                "sizemode": "area",
                # sizeref=calc_sizeref if size else None,
                "sizemin": 4,
            }

        # BORDER: larger bottom/border layer
        if border:
            border_fig = px.scatter_map(
                df_plot,
                lat=lat,
                lon=lon,
                color=color,
                size=size_column,
                size_max=size_max + 8 if size_column is not None else None,
            )
            border_fig.update_traces(marker=border_settings)
            for trace in border_fig.data:
                fig.add_trace(trace)
            # border_trace = px.scatter_map(
            #     df_plot, lat=lat, lon=lon,
            #     color=color,
            #     size=size_column,
            #     size_max=size_max + 8 if size_column is not None else None,
            # ).data[0]
            # border_trace.marker.update(border_settings)
            # fig.add_trace(border_trace)

        # POINT/MARKER: slightly smaller top/fill layer
        fill_fig = px.scatter_map(
            df_plot,
            lat=lat,
            lon=lon,
            opacity=1,  # force settings
            color=color,
            size=size_column,
            size_max=size_max,
            hover_name=hover_name,
            hover_data=hover_data,
        )
        fill_fig.update_traces(marker=marker_settings)
        for trace in fill_fig.data:
            fig.add_trace(trace)
        # fill_trace = px.scatter_map(
        #     df_plot, lat=lat, lon=lon,
        #     opacity=1,  # force settings
        #     color=color,
        #     size=size_column,
        #     size_max=size_max,
        #     hover_name=hover_name,
        #     hover_data=hover_data,

        # ).data[0]
        # fill_trace.marker.update(marker_settings)
        # fig.add_trace(fill_trace)

        return fig

    # ================================================================
    # CUSTOMIZE ANNOTATIONS

    def customize_title(self, title: str, **kwargs):
        upper = kwargs.get("upper", True)

        if upper:
            title = title.upper()

        return [
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(title),
                xref="paper",
                yref="paper",
                x=0.02,
                y=0.98,
                # x=0.,  # Slight padding from the left edge
                # y=1.,  # Slight padding from the top edge
                showarrow=False,
                font=dict(
                    size=16,
                    color="rgba(255, 255, 255, 0.9)",
                    family="'Google Sans', Arial",
                ),
                align="left",
                bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                borderpad=10,
                bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            )
        ]

    def customize_date(self, label: str, **kwargs):
        upper = kwargs.get("upper", False)

        if upper:
            label = label.upper()

        return [
            # Shadow
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(label),
                xref="paper",
                yref="paper",
                x=0.031,
                y=0.967,
                showarrow=False,
                font=dict(
                    size=32, color="rgba(0, 0, 0, 0.2)", family="'Google Sans', Arial"
                ),
                align="left",
                # bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                # borderpad=10,
                # bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            ),
            # Main Text
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(label),
                xref="paper",
                yref="paper",
                x=0.03,
                y=0.97,
                # x=0.,  # Slight padding from the left edge
                # y=1.,  # Slight padding from the top edge
                showarrow=False,
                font=dict(
                    size=32,
                    color="rgba(255, 255, 255, 1)",
                    family="'Google Sans', Arial",
                ),
                align="left",
                # bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                # borderpad=10,
                # bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            ),
        ]

    def customize_hour(self, label: str, **kwargs):
        upper = kwargs.get("upper", False)

        if upper:
            label = label.upper()

        return [
            # Shadow
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(label),
                xref="paper",
                yref="paper",
                x=0.049,
                y=0.105,
                showarrow=False,
                font=dict(
                    size=40, color="rgba(0, 0, 0, 0.2)", family="'Google Sans', Arial"
                ),
                align="left",
                # bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                # borderpad=10,
                # bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            ),
            # Main Text
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(label),
                xref="paper",
                yref="paper",
                x=0.048,
                y=0.109,
                # x=0.,  # Slight padding from the left edge
                # y=1.,  # Slight padding from the top edge
                showarrow=False,
                font=dict(
                    size=40,
                    color="rgba(255, 255, 255, 1)",
                    family="'Google Sans', Arial",
                ),
                align="left",
                # bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                # borderpad=10,
                # bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            ),
        ]

    def customize_label(self, label: str, **kwargs):
        upper = kwargs.get("upper", False)

        if upper:
            label = label.upper()

        return [
            # Shadow
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(label),
                xref="paper",
                yref="paper",
                x=0.981,  # +0.001
                y=0.122,  # -0.003
                showarrow=False,
                font=dict(
                    size=26, color="rgba(0, 0, 0, 0.2)", family="'Google Sans', Arial"
                ),
                align="right",
                # bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                # borderpad=10,
                # bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            ),
            # Main Text
            dict(
                text="<b>&nbsp;{}&nbsp;</b>".format(label),
                xref="paper",
                yref="paper",
                x=0.980,
                y=0.125,
                showarrow=False,
                font=dict(
                    size=26,
                    color="rgba(255, 255, 255, 1)",
                    family="'Google Sans', Arial",
                ),
                align="right",
                # bgcolor="rgba(0, 0, 0, 0.4)",  # Optional: subtle background for readability
                # borderpad=10,
                # bordercolor="rgba(255, 255, 255, 0.9)",
                borderwidth=3,
            ),
        ]

    def smooth_dataframe_for_animation(
        self,
        df: pd.DataFrame,
        date_col,
        location_col,
        lat_col,
        lon_col,
        value_cols,
        freq="10min",
    ):
        """
        Upsamples and interpolates dataframe to create smooth animation frames.
        """
        # Ensure datetime is set correctly and sort initially
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])

        # Store static columns to merge back later
        static_cols = [location_col, lat_col, lon_col]
        static_info = df[static_cols].drop_duplicates(subset=[location_col])

        # Create the new fine-grained time index
        min_time = df[date_col].min()
        max_time = df[date_col].max()

        # CRITICAL FIX 1: Explicitly name the index so it survives reindexing
        new_index = pd.date_range(
            start=min_time, end=max_time, freq=freq, name=date_col
        )

        # We will collect processed frames here
        processed_series = []

        # Calculate how many steps = 1 hour (e.g., 60 / 5 = 12 steps)
        freq_minutes = pd.to_timedelta(freq).total_seconds() / 60
        steps_per_hour = int(60 / freq_minutes)

        for val_col in value_cols:
            # PIVOT: Index=Time, Columns=LocationID, Values=Pollutant
            pivoted = df.pivot(index=date_col, columns=location_col, values=val_col)

            # Reindex to the finer resolution
            pivoted = pivoted.reindex(new_index)

            # INTERPOLATE missing values
            pivoted = pivoted.interpolate(
                method="linear",
                limit_direction="both",
                limit=steps_per_hour,
                limit_area="inside",
            )

            # Unpivot (Melt) back to long format
            melted = pivoted.reset_index().melt(
                id_vars=date_col, var_name=location_col, value_name=val_col
            )

            # set index for easy joining of multiple metrics later
            melted = melted.set_index([date_col, location_col])
            processed_series.append(melted)

        # Combine all metrics
        # Joining on index is faster and safer than iterative merging
        if processed_series:
            interpolated_frames = pd.concat(processed_series, axis=1).reset_index()
        else:
            return pd.DataFrame()  # Return empty if no cols

        # Merge back static info (lat, lon)
        final_df = pd.merge(
            interpolated_frames, static_info, on=location_col, how="left"
        )

        # Drop rows where any value_cols are still NaN
        # final_df = final_df.dropna(subset=value_cols)
        final_df[value_cols] = final_df[value_cols].fillna(0)

        # 5. Create the animation frame string
        final_df["animation_step"] = final_df[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # CRITICAL FIX 2: Sort the final dataframe
        # Plotly animation breaks if frames are not in strict order
        final_df = final_df.sort_values(by=[date_col, location_col])

        return final_df
