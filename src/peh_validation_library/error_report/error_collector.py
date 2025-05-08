from functools import cache


@cache
class ErrorCollector:
    def __init__(self):
        self._errors = []

    def add_error(self, error):
        self._errors.append(error)

    def get_errors(self):
        return self._errors

    def clear_errors(self):
        self._errors.clear()
