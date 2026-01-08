import time
from pprint import pprint
from typing import cast

import duckdb
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from mlforecast import MLForecast
from mlforecast.target_transforms import Differences
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import adfuller
from typing_extensions import Literal
from utilsforecast.evaluation import evaluate
from utilsforecast.losses import mae, mape, rmse

from openaq_anomaly_prediction.core.base_model import BaseModel
from openaq_anomaly_prediction.load.gcp import bq
from openaq_anomaly_prediction.utils.helpers import exec_time
from openaq_anomaly_prediction.utils.logger import ProgressLogger, logger


class TimeSeriesUtils:
    """Time Series Utilities (EDA, preprocessing, etc.)"""

    def __init__(self) -> None:
        pass

    def get_differences_analysis(
        self, df: pd.DataFrame, differences: list[list[int]], **kwargs
    ) -> None:
        """Plot time series and compute ADF statistics for various differencing orders."""
        df = df.copy()
        lags = kwargs.get("lags", [1, 2, 3, 24, 48, 72, 168])
        target_scaler = kwargs.get("target_scaler", None)

        prep_list = []

        # Prepare Original Series for comparison
        original_df = df.copy()
        original_df["unique_id"] = "[ORIGINAL]"
        prep_list.append(original_df)

        # ---------------------------------------------------------------------
        # Process each differencing order and compute ADF statistics

        for d in differences:
            target_transforms: list = [Differences(d)]

            if target_scaler is not None:
                target_transforms.append(target_scaler)

            fcst = MLForecast(
                models=[],  # just preprocessing
                freq="h",
                target_transforms=target_transforms,
                lags=lags,
            )

            # Preprocess applies the Differences(d)
            df_ = fcst.preprocess(df)
            df_ = cast(pd.DataFrame, df_)

            # Change the ID to reflect the differencing (used as title later)
            label = f"Diff {d}"
            df_.loc[:, "unique_id"] = label

            # ADF Test
            self.print_adf_test(df_["y"], label=label)
            # adf_result = adfuller(df_["y"])

            # max_adf_length = len(f"{adf_result[0]:.4f}")
            # p_value_msg = (
            #     "< 1%"
            #     if adf_result[1] < 0.01
            #     else "< 5%"
            #     if adf_result[1] < 0.05
            #     else ">= 5%"
            # )

            # print(
            #     f"{'=' * 64}\n[ {'GOOD' if adf_result[1] < 0.05 else 'BAD'} ] ADF ANALYSIS FOR {label}\n{'-' * 64}"
            # )
            # print(
            #     f"{'ADF Statistic':<19}: {adf_result[0]:.4f} (p-value: {adf_result[1]:.4e} {p_value_msg})"
            # )
            # for key, val in adf_result[4].items():  # type: ignore
            #     tag = "[OK]" if adf_result[0] < val else "[FAIL]"
            #     print(
            #         f"{f'Critical Value {key}':<19}: {val:>{max_adf_length}.4f} {tag}"
            #     )
            # print(f"=> [{'STATIONARY' if adf_result[1] < 0.05 else 'NOT STATIONARY'}]")

            # Correlation of the lagged features with target
            corrs = (
                df_.drop(columns=["unique_id", "ds"])
                .corr()["y"]
                .sort_values(ascending=False, key=abs)
            )
            print("\nTop Correlations (by absolute value):")
            print(corrs)  # Only show top 6 for brevity
            print()

            prep_list.append(df_)

        # ---------------------------------------------------------------------
        # Display all series together with each differencing applied

        unique_ids = [d["unique_id"].iloc[0] for d in prep_list]
        n_series = len(unique_ids)

        # We create a grid: n_series rows, 3 columns
        # Width ratios make the line plot wider than the ACF/PACF squares
        fig = plt.figure(figsize=(18, 3 * n_series))
        gs = fig.add_gridspec(n_series, 3, width_ratios=[5, 2, 2])

        for i, title in enumerate(unique_ids):
            subset = prep_list[i].dropna()

            # 1. Time Series Plot
            ax_line = fig.add_subplot(gs[i, 0])
            ax_line.plot(subset["ds"], subset["y"], color="#1f77b4", alpha=0.8)
            ax_line.set_title(f"{title} - Time Series", loc="left", fontweight="bold")
            ax_line.grid(True, alpha=0.3)

            # 2. ACF Plot
            ax_acf = fig.add_subplot(gs[i, 1])
            plot_acf(subset["y"], ax=ax_acf, lags=60, title=f"{title} ACF")
            ax_acf.set_ylim(
                -1.1, 1.1
            )  # Standardize scale to see over-differencing better

            # 3. PACF Plot
            ax_pacf = fig.add_subplot(gs[i, 2])
            plot_pacf(subset["y"], ax=ax_pacf, lags=60, title=f"{title} PACF")
            ax_pacf.set_ylim(-1.1, 1.1)

        plt.tight_layout()
        plt.show()

    def print_adf_test(self, df_target: pd.Series, **kwargs) -> None:
        """Perform and print the Augmented Dickey-Fuller test results."""

        label = kwargs.get("label", "TARGET SERIE")

        adf_result = adfuller(df_target)

        max_adf_length = len(f"{adf_result[0]:.4f}")
        p_value_msg = (
            "< 1%"
            if adf_result[1] < 0.01
            else "< 5%"
            if adf_result[1] < 0.05
            else ">= 5%"
        )
        stationary_msg = (
            f"=> [{'STATIONARY' if adf_result[1] < 0.05 else 'NOT STATIONARY'}]"
        )

        print(
            f"{'=' * 64}\n[ {'GOOD' if adf_result[1] < 0.05 else 'BAD'} ] ADF ANALYSIS FOR {label}\n{'-' * 64}"
        )
        print(
            f"{'ADF Statistic':<19}: {adf_result[0]:.4f} (p-value: {adf_result[1]:.4e} {p_value_msg})"
        )
        for key, val in adf_result[4].items():  # type: ignore
            tag = "[OK]" if adf_result[0] < val else "[FAIL]"
            print(f"{f'Critical Value {key}':<19}: {val:>{max_adf_length}.4f} {tag}")
        print(stationary_msg)

    def get_mlforecast_burn_in(
        self, segment_size: int, lags=None, lag_transforms=None, target_transforms=None
    ) -> int:
        """Calculate the burn-in (rows lost) for MLForecast based on lags, lag transforms, and target transforms."""

        # 1. Calculate Target Transform Cost (Additive)
        target_cost = 0
        if target_transforms:
            for tfm in target_transforms:
                # Handle nested lists if passed as [ [Differences([24])] ]
                actual_tfms = tfm if isinstance(tfm, list) else [tfm]
                for t in actual_tfms:
                    if hasattr(t, "differences"):
                        target_cost += sum(t.differences)

        # 2. Calculate Lag/Lag Transform Cost (Max of this group)
        lag_costs = [0]
        if lags:
            lag_costs.append(max(lags))

        if lag_transforms:
            for lag, tfms in lag_transforms.items():
                for tfm in tfms:
                    window = getattr(tfm, "window_size", 1)
                    season = getattr(tfm, "season_length", 1)
                    # SeasonalRollingMean uses (window * season) history PLUS the starting lag
                    lag_costs.append(lag + (window * season))

        # 3. Final Burn-in is Additive
        total_burn_in = target_cost + max(lag_costs)
        remaining_rows = segment_size - total_burn_in

        print(f"{'=' * 64}\nMLForecast Burn-in Calculation\n{'-' * 64}")
        print(f"Target Transform Cost: {target_cost}h")
        print(f"Max Lag Feature Cost: {max(lag_costs)}h")
        print(
            f"Total Rows Lost (Burn-in): {total_burn_in}h ({total_burn_in / 24} days)"
        )
        print(
            # f"Rows remaining for training: {remaining_rows}/{segment_size} ({(remaining_rows) / segment_size * 100:.2f}%)\n"
            f"Rows remaining for training: {remaining_rows}h/{segment_size} ({(remaining_rows) / 24:.2f} days)\n"
        )
        return total_burn_in

        # costs = [0]

        # # Cost of simple lags
        # if lags:
        #     costs.append(max(lags))

        # # Cost of lag transforms
        # if lag_transforms:
        #     for lag, tfms in lag_transforms.items():
        #         for tfm in tfms:
        #             # Get window_size and season_length if they exist
        #             window = getattr(tfm, "window_size", 1)
        #             season = getattr(tfm, "season_length", 1)
        #             costs.append(lag + (window * season))

        # # Cost of target transforms (Differences)
        # if target_transforms:
        #     for tfm in target_transforms:
        #         if hasattr(tfm, "differences"):
        #             costs.append(sum(tfm.differences))

        # burn_in = max(costs)

        # print(f"{'=' * 64}\nMLForecast Burn-in Calculation\n{'-' * 64}")
        # print(f"Rows lost (Burn-in): {burn_in} ({burn_in / 24} days)")
        # print(
        #     f"Rows remaining for training: {segment_size - burn_in}/{segment_size} ({(segment_size - burn_in) / segment_size * 100:.2f}%)\n"
        # )

        # return burn_in

    def print_cv_evaluation(
        self, model_name: str, cv_results: pd.DataFrame
    ) -> pd.DataFrame:
        """Compute evaluation metrics for All Data and Peaks (Top 10%) across unique_ids."""

        SEP_LENGTH = 64

        # Add residuals column if not present
        residuals_col = f"{model_name}_residuals"
        if residuals_col not in cv_results.columns:
            cv_results[residuals_col] = cv_results["y"] - cv_results[model_name]

        # Global min/max for range calculations
        y_min = cv_results["y"].min()
        y_max = cv_results["y"].max()
        y_range = y_max - y_min

        # Define Peaks (Top 10% of actual values)
        peak_threshold = cv_results["y"].quantile(0.90)
        peak_df = cv_results[cv_results["y"] > peak_threshold].copy()

        # Compute Metrics for ALL Data (Standard)
        evaluation = evaluate(
            cv_results.drop(columns="cutoff", errors="ignore"),
            metrics=[rmse, mae, mape],
        )

        # Compute Metrics for PEAK Data
        evaluation_peak = evaluate(
            peak_df.drop(columns="cutoff", errors="ignore"),
            metrics=[rmse, mae, mape],
        )
        # Rename metrics in the peak dataframe to distinguish them (e.g., "rmse" -> "peak_rmse")
        evaluation_peak["metric"] = evaluation_peak["metric"].apply(
            lambda x: f"peak_{x}"
        )

        # Combine, aggregate and group by both evaluations
        final_eval = pd.concat([evaluation, evaluation_peak])
        final_eval = final_eval.sort_values(by=["metric", "unique_id", model_name])
        summary = final_eval.groupby("metric").agg({model_name: ["mean", "std"]})

        # Build the 1-row report
        report = pd.DataFrame(index=[model_name])

        # Define the specific order of columns we want in the final report
        metrics_order = ["rmse", "peak_rmse", "peak_mae", "mae", "mape", "peak_mape"]
        for met in metrics_order:
            if met in summary.index:
                m_val = summary.loc[met, (model_name, "mean")]
                s_val = summary.loc[met, (model_name, "std")]

                # Mean/Std
                report[f"{met}_mean"] = m_val
                report[f"{met}_std"] = s_val

                # Variance Score: std relative to mean
                report[f"{met}_variance_%"] = (s_val / m_val) if m_val != 0 else 0

                # Error range (how big is the error compared to the global data range)
                if "rmse_mean" in report.columns:
                    report["rmse_range_ratio_%"] = report["rmse_mean"] / y_range
                if "peak_rmse_mean" in report.columns:
                    report["peak_rmse_range_ratio_%"] = (
                        report["peak_rmse_mean"] / y_range
                    )
                # report[f"peak_{met}_range_ratio_%"] = (
                #     report[f"peak_{met}_mean"] / y_range
                # )
        # Print context about what defined a "Peak"
        # print(f"[{model_name}] Peak Threshold (>90%): {peak_threshold:.4f} | Peak Samples: {len(peak_df)}")

        # Display only RMSE and Peak RMSE metrics for brevity
        selected_columns = [
            col for col in report.columns if col.startswith(("rmse", "peak_rmse"))
        ]
        selected_metrics = report.loc[:, selected_columns]

        # try:
        #     display(report[selected_metrics.columns])  # type: ignore
        # except NameError:
        #     print(report[selected_metrics.columns])

        # -------------------------------------------------------------------------
        # NEW: STABILITY CHECK (Normalized RMSE)
        # -------------------------------------------------------------------------
        # 1. Calculate RMSE per ID (already in 'evaluation' variable filtered for RMSE)
        rmse_per_id = evaluation[evaluation["metric"] == "rmse"].copy()

        # 2. Calculate the Mean Target Value per ID (The Scale)
        mean_per_id = cv_results.groupby("unique_id")["y"].mean().reset_index()

        # 3. Merge to get "RMSE" and "Mean Y" in same row
        rmse_per_id = rmse_per_id.merge(mean_per_id, on="unique_id")

        # 4. Calculate Normalized RMSE (NRMSE)
        # "Error as a percentage of the average pollution at this location"
        rmse_per_id["nrmse"] = rmse_per_id[model_name] / rmse_per_id["y"]

        # 5. Check Stability of the NORMALIZED score
        nrmse_mean = rmse_per_id["nrmse"].mean()
        nrmse_std = rmse_per_id["nrmse"].std()
        nrmse_variance_pct = (nrmse_std / nrmse_mean) if nrmse_mean != 0 else 0

        # Print report
        title = f"Cross-Validation Report: [ {report.at[model_name, 'rmse_mean']:.4f} ] RMSE ({model_name})"
        print(f"{'=' * SEP_LENGTH}\n{title}\n{'-' * SEP_LENGTH}")
        # print(f"Total segments: {final_eval['unique_id'].nunique()}\n")

        selected_metrics = ["rmse", "peak_rmse", "mae", "mape"]
        max_length_metric = max(len(metric) for metric in selected_metrics)
        max_length_value = len(str(int(report.values.max()))) + 5  # for formatting
        for metric in selected_metrics:
            # if metric in ["mae"]:
            #     print()

            # if f"{metric}_mean" in report.columns:
            mean_val = report.at[model_name, f"{metric}_mean"]
            std_val = report.at[model_name, f"{metric}_std"]
            variance_pct = report.at[model_name, f"{metric}_variance_%"]
            range_ratio_pct = (
                report.at[model_name, f"{metric}_range_ratio_%"]
                if metric in ["rmse", "peak_rmse"]
                else None
            )
            range_ratio_str = (
                f"  |  {range_ratio_pct:>6.2%} (error_range_%)"
                if range_ratio_pct is not None
                else ""
            )
            print(
                f"{f'[{metric.upper()}]':<{max_length_metric + 2}} : {mean_val:>{max_length_value}.4f} mean, {std_val:>{max_length_value}.4f} std  |  {variance_pct:>6.2%} (variance_%){range_ratio_str}"
            )

        print(f"{'-' * SEP_LENGTH}")
        print(f"STABILITY TRUTH CHECK (Normalized by Sensor Scale):")
        print(f"NRMSE Variance:      {nrmse_variance_pct:.2%}  <-- TRUST THIS NUMBER")
        print(f"{'=' * SEP_LENGTH}\n")

        print()

        return final_eval

    def print_detailed_cv_evaluation(
        self,
        model_name: str,
        cv_result: pd.DataFrame,
        evaluation: pd.DataFrame,
        **kwargs,
    ) -> None:
        """Print detailed cross-validation evaluation for random unique_ids."""

        show_adf: bool = kwargs.get("show_adf", True)
        figsize: tuple[int, int] = kwargs.get("figsize", (16, 8))
        minmax_scaled: bool = kwargs.get("minmax_scaled", False)
        max_samples = kwargs.get("max_samples", 8)
        random_state: int = kwargs.get("random_state", int(time.time()))
        SEP_LENGTH = 64

        # Get a random sample of unique_ids to display
        rng = np.random.default_rng(seed=random_state)
        random_unique_ids = rng.choice(
            cv_result["unique_id"].unique(), size=max_samples, replace=False
        )
        # print(f"Random Unique IDs for Detailed Evaluation: {random_unique_ids}")

        subtitle = ""
        if show_adf:
            stationary_uids = []
            for uid in cv_result["unique_id"].unique():
                adf_result = adfuller(
                    cv_result[cv_result["unique_id"] == uid][f"{model_name}_residuals"]
                )
                if adf_result[1] < 0.05:
                    stationary_uids.append(uid)
            subtitle = f"Stationary Unique IDs (ADF p-value < 0.05): {len(stationary_uids)} ({len(stationary_uids) / len(cv_result['unique_id'].unique()):.2%})\n"

        title = "Detailed Cross-Validation Evaluation"
        print(f"{'=' * SEP_LENGTH}\n{title}\n{'-' * SEP_LENGTH}{subtitle}")

        for i, unique_id in enumerate(random_unique_ids):
            unique_id_metrics = evaluation[evaluation["unique_id"] == unique_id].copy()
            uid_cv_result = cv_result[cv_result["unique_id"] == unique_id].copy()

            adf_result = adfuller(uid_cv_result[f"{model_name}_residuals"])
            p_value = adf_result[1]

            # if p_value < 0.05:
            #     stationary_uids.append(unique_id)

            # stationary_msg = f"[{'STATIONARY' if p_value < 0.05 else 'NOT STATIONARY'}]"
            adf_threshold = (
                "p-value < 1%"
                if p_value < 0.01
                else "p-value < 5%"
                if p_value < 0.05
                else "p-value > 5%"
            )
            metrics_pretty = {
                row[1]["metric"]: row[1][f"{model_name}"]
                for row in unique_id_metrics.iterrows()
            }
            # metrics_pretty_msg = f"[RMSE]: {metrics_pretty['rmse']:.4f}, [MAE]: {metrics_pretty['mae']:.4f}, [MAPE]: {metrics_pretty['mape']:.2%}"
            metrics_pretty_msg = f"[ {metrics_pretty['rmse']:.4f} rmse ][ {metrics_pretty['mae']:.4f} mae ][ {metrics_pretty['mape']:.2%} mape ]"
            # print(
            #     f"{'=' * SEP_LENGTH}\n{unique_id}: {metrics_pretty_msg}\n{'-' * SEP_LENGTH}"
            # )
            # display(unique_id_metrics.reset_index(drop=True))

            # for row in unique_id_metrics.iterrows():
            #     print(f"[{row[1]['metric'].upper()}]: {row[1][f'{model_name}']:.4f}")

            # pivoted_metrics = (
            #     unique_id_metrics.pivot(index="unique_id", columns="metric", values=f"{model_name}")
            #     .reset_index()
            #     .rename_axis(None, axis=1)
            # )
            # display(pivoted_metrics)

            color = (
                "#2ca02c"
                if p_value < 0.01
                else "#e0a930"
                if p_value < 0.05
                else "#b13c3c"
            )

            fig = plt.figure(figsize=figsize)
            gs = fig.add_gridspec(2, 2, width_ratios=[5, 4], height_ratios=[3, 2])

            fig.suptitle(
                metrics_pretty_msg,
                fontsize=14,
                fontweight="bold",
                x=0.05,  # Moves it to the left (0 is far left, 1 is far right)
                horizontalalignment="left",
                color=color,
            )

            fig.text(
                0.99,
                0.98,
                # f"ADF Score of Residuals: {stationary_msg} {p_value:.4e}",
                f"ADF Score of Residuals: {p_value:.4%} ({adf_threshold})",
                fontsize=13,
                fontweight="normal",
                ha="right",
                va="top",
                color="grey",
            )

            # Predictions vs Actuals
            ax_predictions = fig.add_subplot(gs[0, 0])
            sns.lineplot(
                data=uid_cv_result,
                x="ds",
                y="y",
                ax=ax_predictions,
                label="Actual",
                # color="#BBBBBB",
                alpha=0.7,
            )
            sns.lineplot(
                data=uid_cv_result,
                x="ds",
                y=f"{model_name}",
                ax=ax_predictions,
                label=model_name.upper(),
                # color=color,
            )
            ax_predictions.set_title(f"Actual vs Predictions: {unique_id}")
            ax_predictions.grid(True, alpha=0.3)
            ax_predictions.legend()
            if minmax_scaled:
                max_value = (
                    cv_result[cv_result["unique_id"].isin(random_unique_ids)][
                        ["y", f"{model_name}"]
                    ]
                    .max()
                    .max()
                )
                min_value = (
                    cv_result[cv_result["unique_id"].isin(random_unique_ids)][
                        ["y", f"{model_name}"]
                    ]
                    .min()
                    .min()
                )
                value_range = max_value - min_value
                y_margin = value_range * 0.1  # 10% margin
                ax_predictions.set_ylim(min_value - y_margin, max_value + y_margin)

            # Residuals plot
            ax_residuals = fig.add_subplot(gs[0, 1])
            sns.scatterplot(
                data=uid_cv_result, x="ds", y=f"{model_name}_residuals", ax=ax_residuals
            )
            ax_residuals.set_title(f"Residuals (Actual - Predicted): {unique_id}")
            ax_residuals.axhline(
                0, color="black", linestyle="--", alpha=0.5
            )  # Zero-error
            ax_residuals.grid(True, alpha=0.3)

            ax_acf = fig.add_subplot(gs[1, 0])
            uid_residuals = uid_cv_result[f"{model_name}_residuals"].dropna()
            plot_acf(
                uid_residuals,
                ax=ax_acf,
                lags=60,
                title="Residual Autocorrelation (ACF)",
            )
            ax_acf.set_ylim(-1.1, 1.1)

            ax_pacf = fig.add_subplot(gs[1, 1])
            uid_residuals = uid_cv_result[f"{model_name}_residuals"].dropna()
            plot_pacf(
                uid_residuals,
                ax=ax_pacf,
                lags=36,
                title="Residual Partial Autocorrelation (PACF)",
            )
            ax_pacf.set_ylim(-1.1, 1.1)

            plt.tight_layout()
            # plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            plt.show()
            plt.close(fig)

            if i >= max_samples:
                break


tsu = TimeSeriesUtils()


# new_rows = pd.DataFrame(
#     [
#         {
#             "metric": "y",
#             f"{model_name}_mean": cv_results["y"].mean(),
#             f"{model_name}_std": None,
#         },
#         {
#             "metric": model_name,
#             f"{model_name}_mean": cv_results[model_name].mean(),
#             f"{model_name}_std": None,
#         },
#     ]
# )
# model_metrics = pd.concat([model_metrics, new_rows], ignore_index=True)
