from enum import StrEnum


class ErrorLevel(StrEnum):
    WARNING = 'warning'
    ERROR = 'error'
    CRITICAL = 'critical'


class ValidationType(StrEnum):
    DATE = 'date'
    DATETIME = 'datetime'
    BOOL = 'boolean'
    FLOAT = 'decimal'
    INT = 'integer'
    STR = 'varchar'
    CAT = 'categorical'


class CheckCases(StrEnum):
    CONDITION = 'condition'
    CONJUNCTION = 'conjunction'
    DISJUNCTION = 'disjunction'
