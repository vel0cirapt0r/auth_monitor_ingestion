import logging
import sys
import os
from datetime import datetime
import structlog
from structlog.stdlib import BoundLogger
import colorama
from colorama import Fore, Style
from ingest.config import LOG_LEVEL

colorama.init()

# Custom log levels (added below standard NOTSET=0)
logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.DEBUG1 = 10  # Alias for standard DEBUG
logging.addLevelName(logging.DEBUG1, "DEBUG1")
logging.DEBUG2 = 15
logging.addLevelName(logging.DEBUG2, "DEBUG2")
logging.DEBUG3 = 18
logging.addLevelName(logging.DEBUG3, "DEBUG3")

# Extend logger class to support custom methods
def trace(self, msg, *args, **kw):
    self._log(logging.TRACE, msg, args, **kw)
logging.Logger.trace = trace

def debug1(self, msg, *args, **kw):
    self._log(logging.DEBUG1, msg, args, **kw)
logging.Logger.debug1 = debug1

def debug2(self, msg, *args, **kw):
    self._log(logging.DEBUG2, msg, args, **kw)
logging.Logger.debug2 = debug2

def debug3(self, msg, *args, **kw):
    self._log(logging.DEBUG3, msg, args, **kw)
logging.Logger.debug3 = debug3

class ColoredJSONFormatter(structlog.processors.JSONRenderer):
    """Custom JSON formatter with colors for console."""
    color_map = {
        "TRACE": Fore.CYAN,
        "DEBUG1": Fore.BLUE,
        "DEBUG2": Fore.BLUE + Style.DIM,
        "DEBUG3": Fore.BLUE + Style.BRIGHT,
        "DEBUG": Fore.BLUE,  # Fallback for standard DEBUG
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Style.BRIGHT,
    }

    def __call__(self, logger, name, event_dict):
        json_str = super().__call__(logger, name, event_dict)
        level = event_dict.get("level", "").upper()
        color = self.color_map.get(level, '')
        return color + json_str + Style.RESET_ALL

# Log file setup
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, f"{datetime.now().isoformat()}.log")

# Stdlib handlers
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(structlog.stdlib.ProcessorFormatter(
    processor=structlog.processors.JSONRenderer(),
))
file_handler.setLevel(logging.TRACE)  # File always logs everything >= TRACE

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(structlog.stdlib.ProcessorFormatter(
    processor=ColoredJSONFormatter(),
))
console_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
console_handler.setLevel(console_level)

# Structlog config with stdlib integration
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.CallsiteParameterAdder({
            structlog.processors.CallsiteParameter.FUNC_NAME,
        }),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

# Root logger setup
logging.basicConfig(handlers=[file_handler, console_handler], level=console_level)

logger: BoundLogger = structlog.get_logger()
