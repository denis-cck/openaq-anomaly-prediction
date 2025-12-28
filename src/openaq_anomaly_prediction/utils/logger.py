import sys
from datetime import datetime

from loguru import logger

# TODO: Move to a config file
CURRENT_DEBUG_LEVEL = "TRACE"
VERBOSE = False

SEVERITY_LEVELS = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR"]
LEVEL_MAX_LENGTH = 10


def print_newline(level="INFO") -> None:
    """
    Print a newline only if the current debug level is
    equal or higher than the specified level.
    """
    if SEVERITY_LEVELS.index(level) >= SEVERITY_LEVELS.index(CURRENT_DEBUG_LEVEL):
        print()


logger.level("TRACE", color="<fg #666666>")
# logger.level("DEBUG", color="<fg #00FF00>")
# logger.level("INFO", color="<fg #33CCFF>")
# logger.level("WARNING", color="<fg #FFA500><bold>")
# logger.level("ERROR", color="<fg #FF0000><bg #110000><bold>")


def loguru_custom_fmt(record):
    """Custom loguru format function to add more context."""
    message_string = "<white>{message}</white>"
    file_string = "<k><d> [{name}:{line}]</d></k>"

    if record["level"].name == "TRACE":
        # message_string = "<k><d> 󰘍 {message}</d></k>"
        message_string = "<fg #666666> 󰘍 {message}</fg #666666>"
        file_string = ""

    # elif record["level"].name == "DEBUG":
    #     message_string = "<k>{message}</k>"

    elif record["level"].name == "SUCCESS":
        message_string = "<w><u><b>{message}</b></u></w>"

    elif record["level"].name == "ERROR":
        message_string = "<w><R><b>{message}</b></R></w>"

    if not VERBOSE:
        file_string = ""

    return f"<fg #666666>{{time:%H:%M:%S}}</fg #666666> <lvl>{{level:>{LEVEL_MAX_LENGTH}}} <b>|</b>  </lvl>{message_string}{file_string}\n{{exception}}"


logger.configure(
    handlers=[
        {
            "sink": sys.stdout,
            "colorize": True,
            "format": loguru_custom_fmt,
            "level": CURRENT_DEBUG_LEVEL,
        }
    ]
)


class ProgressLogger:
    # https://colordesigner.io/gradient-generator

    # #666866 → #2BD683, OKLCH/shorter
    _GRADIENT = ["#666666", "#896e69", "#a47b58", "#ab933f", "#92b446"]
    # _GRADIENT = ["#666666", "#57826D", "#499E75", "#3ABA7C", "#2BD683"]
    # _GRADIENT = ["#666666", "#7a5d71", "#77528f", "#464fa6", "#39a2be"]
    _GRADIENT_END = "#2BD683"
    _GRADIENT_STEPS = 1 / len(_GRADIENT)

    # _GRADIENT = ["#666666", "#57826D", "#499E75", "#3ABA7C", "#2BD683"]
    _TIME_GRADIENT = ["#666666", "#666666", "#76625e", "#876854", "#98784a"]
    _TIME_GRADIENT += ["#ab933f", "#b0813b", "#b66c37", "#bb5332", "#c1372e"]
    _TIME_GRADIENT_END = "#c1372e"
    # _TIME_GRADIENT = ["#666666", "#735959", "#804d4c", "#8e413e", "#9b3631"]
    # _TIME_GRADIENT_END = "#D7433A"
    # _TIME_GRADIENT = ["#666666", "#666666", "#7b5b5a", "#914f4d", "#a8433e"]
    # _TIME_GRADIENT_END = "#C1372E"
    _TIME_GRADIENT_STEPS = 1 / len(_TIME_GRADIENT)

    def __init__(self) -> None:
        # Track the length of the last progress line so we can fully clear it even when the
        # new message is shorter (helps in notebook/stdout environments with no terminal width).
        self._last_progress_len = 0

    # ---------------------------------------------------------------------
    # STATIC METHODS

    @staticmethod
    def time_gradient(text: str, current_time: float, max_time: float) -> str:
        """Return text colored with a gradient based on progress."""

        # c = hex("#666666")
        progress_pct = current_time / max_time

        if progress_pct >= 1:
            color_code = ProgressLogger._TIME_GRADIENT_END
        else:
            gradient_index = int(progress_pct / ProgressLogger._TIME_GRADIENT_STEPS)
            color_code = ProgressLogger._TIME_GRADIENT[gradient_index]

        return f"{b()}{hex(color_code)}{text}{rst()}{grey()}"

    @staticmethod
    def text_gradient(text: str, current_progress: float, total_progress: float) -> str:
        """Return text colored with a gradient based on progress."""

        # c = grey()
        progress_pct = current_progress / total_progress

        if progress_pct >= 1:
            color_code = ProgressLogger._GRADIENT_END
        else:
            gradient_index = int(progress_pct / ProgressLogger._GRADIENT_STEPS)
            color_code = ProgressLogger._GRADIENT[gradient_index]

        return f"{hex(color_code)}{text}{rst()}{grey()}"

    # ---------------------------------------------------------------------
    # PUBLIC METHODS

    def print(self, message: str, **kwargs) -> None:
        """Print a progress log."""

        current_progress = kwargs.get("current_progress", 0)
        total_progress = kwargs.get("total_progress", 0)
        prefix_msg = kwargs.get("prefix_msg", None)
        # suffix_msg = kwargs.get("suffix_msg", None)
        last = kwargs.get("last", False)

        now = datetime.now().strftime("%H:%M:%S")
        prefix = "" if prefix_msg is None else prefix_msg  # default
        # suffix = "" if suffix_msg is None else suffix_msg  # default

        progress_str = f"{grey()}{now:<9}{prefix:>{LEVEL_MAX_LENGTH}} |   󰘍 {rst()}"

        if total_progress > 0:
            progress_pct = current_progress / total_progress

            if progress_pct >= 1:
                color_code = ProgressLogger._GRADIENT_END
            else:
                gradient_index = int(progress_pct / ProgressLogger._GRADIENT_STEPS)
                color_code = ProgressLogger._GRADIENT[gradient_index]
            progress_str += (
                f"{b()}{hex(color_code)}{progress_pct:>4.0%}{rst()}{grey()}: {rst()}"
            )

        progress_str += f"{grey()}{message}{rst()}"

        # Ensure the line is fully cleared even if the new message is shorter than the previous one
        padding = max(self._last_progress_len - len(progress_str), 0)
        self._last_progress_len = len(progress_str)

        print(f"\r{clr()}{progress_str}{' ' * padding}", end="", flush=True)
        # print(f"{progress_str}{' ' * padding}{clr()}", end="\r", flush=True)

        if last:
            print()
            # self.end()

    # def end(self) -> None:
    #     """End the progress log with a newline."""
    #     print()


def b() -> str:
    """Return a Bold ANSI code."""
    return "\x1b[1m"


def grey() -> str:
    """Return a default Grey ANSI code."""
    return hex("#666666")


def hex(color_code: str) -> str:
    """Return an ANSI code for a given hex color."""
    hex_string = color_code.lstrip("#")
    return f"\x1b[38;2;{int(hex_string[0:2], 16)};{int(hex_string[2:4], 16)};{int(hex_string[4:6], 16)}m"


def rst() -> str:
    """Return a Reset ANSI code."""
    return "\x1b[0m"


def clr() -> str:
    """Return a Clear Line ANSI code."""
    return "\x1b[K"


if __name__ == "__main__":
    logger.trace("This is a trace message.")
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.success("This is a success message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
