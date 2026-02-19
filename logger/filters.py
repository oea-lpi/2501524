import logging

class MaxLevelFilter(logging.Filter):
    """
    Allow records up to and including max_level (int).
    DEBUG=10, INFO=20...
    """
    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level