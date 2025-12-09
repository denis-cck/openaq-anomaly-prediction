import sys

from loguru import logger

# TODO: Move to a config file
CURRENT_DEBUG_LEVEL = "TRACE"
VERBOSE = False

SEVERITY_LEVELS = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR"]


def print_newline(level="INFO") -> None:
    """
    Print a newline only if the current debug level is
    equal or higher than the specified level.
    """
    if SEVERITY_LEVELS.index(level) >= SEVERITY_LEVELS.index(CURRENT_DEBUG_LEVEL):
        print()


logger.level("TRACE", color="<k>")
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
        message_string = "<k> 󰘍 {message}</k>"
        file_string = ""

    # elif record["level"].name == "DEBUG":
    #     message_string = "<k>{message}</k>"

    elif record["level"].name == "SUCCESS":
        message_string = "<w><u><b>{message}</b></u></w>"

    elif record["level"].name == "ERROR":
        message_string = "<w><R><b>{message}</b></R></w>"

    if not VERBOSE:
        file_string = ""

    return f"<k>{{time:%H:%M:%S}}</k> <lvl>{{level:>8}} <b>|</b>  </lvl>{message_string}{file_string}\n{{exception}}"


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


if __name__ == "__main__":
    logger.trace("This is a trace message.")
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.success("This is a success message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
