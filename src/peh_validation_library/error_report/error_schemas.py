from pydantic import BaseModel

from peh_validation_library.core.utils.enums import ErrorLevel


class ExceptionSchema(BaseModel):
    error_type: str
    error_message: str
    error_level: ErrorLevel
    error_traceback: str
    error_context: str | None = None
    error_source: str | None = None
