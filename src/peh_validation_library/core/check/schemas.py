from __future__ import annotations

from typing import Any, Callable

import pandera.polars as pa
import polars as pl
from pydantic import BaseModel, Field

from peh_validation_library.core.utils.enums import CheckCases
from peh_validation_library.core.utils.mappers import (
    expression_mapper,
)


class SimpleCheckExpression(BaseModel):
    command: str | Callable[[pa.PolarsData, Any], pl.LazyFrame]
    subject: list[str] | None = None
    arg_values: list[Any] | None = None
    arg_columns: list[str] | None = None

    def get_check_name(self) -> str:
        try:
            return self.command.replace('_', ' ').title()
        except AttributeError:
            return self.command.__name__.replace('_', ' ').title()

    def get_message(self) -> str:
        msg = f'The column {self.get_check_name()}'

        if self.subject:
            msg = f'Column(s) {self.subject} {self.get_check_name()}'
        if self.arg_values:
            msg += f' {self.arg_values}'
        elif self.arg_columns:
            msg += f' {self.arg_columns}'
        return msg

    def map_command(self) -> str:
        self.command = expression_mapper[self.command]

    def get_args(self) -> dict[str, Any]:
        args = {}
        if self.subject:
            args['subject'] = self.subject
        if self.arg_values:
            args['arg_values'] = self.arg_values
        if self.arg_columns:
            args['arg_columns'] = self.arg_columns
        return args


class CaseCheckExpression(BaseModel):
    check_case: CheckCases
    expressions: list[SimpleCheckExpression | CaseCheckExpression] = Field(
        min_length=2, max_length=2
    )

    def get_check_name(self) -> str:
        return (
            f'{str(self.check_case.name).title()} of '
            f'{", ".join([e.get_check_name() for e in self.expressions])}'
        )

    def get_message(self) -> str:
        return f'{", ".join([e.get_message() for e in self.expressions])}'

    def get_args(self) -> dict[str, Any]:
        args = []
        for exp in self.expressions:
            args.append(exp.get_args())
        return args
