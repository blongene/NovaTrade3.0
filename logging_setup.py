# utils_logging.py (or at top of utils.py)
import logging, os, sys

def init_logging():
    level = os.getenv("NOVA_LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    root.setLevel(level)

    # stdout handler: INFO and below
    h_out = logging.StreamHandler(sys.stdout)
    h_out.setLevel(logging.DEBUG)
    h_out.addFilter(lambda r: r.levelno <= logging.INFO)

    # stderr handler: WARNING and above
    h_err = logging.StreamHandler(sys.stderr)
    h_err.setLevel(logging.WARNING)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    h_out.setFormatter(fmt)
    h_err.setFormatter(fmt)

    # reset handlers (avoid dupes on redeploy)
    root.handlers[:] = [h_out, h_err]

    # convenience child logger
    return logging.getLogger("nova")
