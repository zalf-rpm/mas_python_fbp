# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)
"""Safe arithmetic expressions, e.g. for derived calibration parameters.

An expression is parsed once with ``ast`` and checked against a whitelist before it is ever
evaluated, so a bad expression fails when the configuration is loaded, not halfway through a run.
Evaluation then walks the checked tree itself; nothing is handed to ``eval``.

Allowed:

- number and string literals, list and tuple literals
- names given up front (``names``) plus the functions in ``FUNCTIONS``
- ``+ - * / // % **`` (``**`` is always computed as a float), unary ``- + not``, comparisons,
  ``and``/``or``, ``x if cond else y``
- subscripts with a single index or key: ``cultivar["StageTemperatureSum"][0][2]``, ``v[3]``
- calls to the functions in ``FUNCTIONS``, positional arguments only

Everything else is rejected, attribute access (``.``) in particular: it is what every escape out
of a restricted ``eval`` goes through (``().__class__.__base__.__subclasses__()``). Arithmetic
only takes numbers, so ``"a" * 10**9`` or ``[0] * 10**9`` cannot be used to exhaust memory, and
``**`` refuses exponents above ``MAX_EXPONENT``.
"""

from __future__ import annotations

import ast
import math
import operator as op
from collections.abc import Callable, Iterable, Mapping
from typing import Any

MAX_EXPRESSION_LENGTH = 1000
MAX_EXPONENT = 100

_BIN_OPS: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: op.add,
    ast.Sub: op.sub,
    ast.Mult: op.mul,
    ast.Div: op.truediv,
    ast.FloorDiv: op.floordiv,
    ast.Mod: op.mod,
    ast.Pow: op.pow,
}
_UNARY_OPS: dict[type, Callable[[Any], Any]] = {ast.USub: op.neg, ast.UAdd: op.pos, ast.Not: op.not_}
_CMP_OPS: dict[type, Callable[[Any, Any], Any]] = {
    ast.Lt: op.lt,
    ast.LtE: op.le,
    ast.Gt: op.gt,
    ast.GtE: op.ge,
    ast.Eq: op.eq,
    ast.NotEq: op.ne,
}


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


FUNCTIONS: dict[str, Callable[..., Any]] = {
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "sum": sum,
    "len": len,
    "sqrt": math.sqrt,
    "exp": math.exp,
    "log": math.log,
    "log10": math.log10,
    "clip": _clip,
}


class ExpressionError(ValueError):
    """An expression that is not allowed, or that failed to evaluate."""


def _is_number(x: Any) -> bool:
    # bool is an int, and numpy's float64 a float, which is both what we want here
    return isinstance(x, (int, float))


class Expression:
    """A checked expression, evaluated against a mapping of names to values."""

    def __init__(self, source: str, names: Iterable[str]):
        self.source = source.strip()
        if not self.source:
            msg = "empty expression"
            raise ExpressionError(msg)
        if len(self.source) > MAX_EXPRESSION_LENGTH:
            msg = f"expression longer than {MAX_EXPRESSION_LENGTH} characters"
            raise ExpressionError(msg)
        try:
            self._tree = ast.parse(self.source, mode="eval").body
        except SyntaxError as e:
            msg = f"'{self.source}' is not a valid expression: {e.msg}"
            raise ExpressionError(msg) from None
        self.names = set(names)
        self.used_names: set[str] = set()
        self._check(self._tree)

    def _reject(self, node: ast.AST, why: str = "not allowed") -> ExpressionError:
        snippet = ast.get_source_segment(self.source, node) or type(node).__name__
        return ExpressionError(f"in '{self.source}': '{snippet}' is {why}")

    def _check(self, node: ast.AST) -> None:
        match node:
            case ast.Constant(value=v) if isinstance(v, (int, float, str)) and not isinstance(v, complex):
                pass
            case ast.Name(id=name, ctx=ast.Load()):
                if name in FUNCTIONS:
                    raise self._reject(node, "a function and can only be called")
                if name not in self.names:
                    raise self._reject(node, "an unknown name")
                self.used_names.add(name)
            case ast.BinOp(left=left, op=bin_op, right=right) if type(bin_op) in _BIN_OPS:
                self._check(left)
                self._check(right)
            case ast.UnaryOp(op=un_op, operand=operand) if type(un_op) in _UNARY_OPS:
                self._check(operand)
            case ast.Compare(left=left, ops=ops, comparators=comparators) if all(type(o) in _CMP_OPS for o in ops):
                for n in [left, *comparators]:
                    self._check(n)
            case ast.BoolOp(values=values):
                for n in values:
                    self._check(n)
            case ast.IfExp(test=test, body=body, orelse=orelse):
                for n in (test, body, orelse):
                    self._check(n)
            case ast.Subscript(value=value, slice=index, ctx=ast.Load()) if not isinstance(
                index, (ast.Slice, ast.Tuple)
            ):
                self._check(value)
                self._check(index)
            case ast.List(elts=elts, ctx=ast.Load()) | ast.Tuple(elts=elts, ctx=ast.Load()):
                for n in elts:
                    if isinstance(n, ast.Starred):
                        raise self._reject(n)
                    self._check(n)
            case ast.Call(func=ast.Name(id=fname), args=args, keywords=[]) if fname in FUNCTIONS:
                for n in args:
                    if isinstance(n, ast.Starred):
                        raise self._reject(n)
                    self._check(n)
            case ast.Attribute():
                raise self._reject(node, "attribute access, which is not allowed")
            case _:
                raise self._reject(node)

    def __call__(self, values: Mapping[str, Any]) -> Any:
        try:
            return self._eval(self._tree, values)
        except ExpressionError:
            raise
        except (ArithmeticError, ValueError, TypeError, KeyError, IndexError) as e:
            msg = f"evaluating '{self.source}' failed: {type(e).__name__}: {e}"
            raise ExpressionError(msg) from None

    def _eval(self, node: ast.AST, values: Mapping[str, Any]) -> Any:
        match node:
            case ast.Constant(value=v):
                return v
            case ast.Name(id=name):
                if name not in values:
                    msg = f"evaluating '{self.source}' failed: no value for '{name}'"
                    raise ExpressionError(msg)
                return values[name]
            case ast.BinOp(left=left, op=bin_op, right=right):
                lhs, rhs = self._eval(left, values), self._eval(right, values)
                if not (_is_number(lhs) and _is_number(rhs)):
                    raise self._reject(node, "arithmetic on something other than numbers")
                if isinstance(bin_op, ast.Pow):
                    if abs(rhs) > MAX_EXPONENT:
                        raise self._reject(node, f"a power with an exponent above {MAX_EXPONENT}")
                    # as floats: nested integer powers would otherwise grow without bound
                    # (((10**100)**100)**100 ...), where a float simply overflows
                    return float(lhs) ** rhs
                return _BIN_OPS[type(bin_op)](lhs, rhs)
            case ast.UnaryOp(op=un_op, operand=operand):
                val = self._eval(operand, values)
                if not isinstance(un_op, ast.Not) and not _is_number(val):
                    raise self._reject(node, "arithmetic on something other than numbers")
                return _UNARY_OPS[type(un_op)](val)
            case ast.Compare(left=left, ops=ops, comparators=comparators):
                lhs = self._eval(left, values)
                for cmp_op, comparator in zip(ops, comparators):
                    rhs = self._eval(comparator, values)
                    if not _CMP_OPS[type(cmp_op)](lhs, rhs):
                        return False
                    lhs = rhs
                return True
            case ast.BoolOp(op=bool_op, values=operands):
                result: Any = None
                for n in operands:
                    result = self._eval(n, values)
                    if isinstance(bool_op, ast.And) and not result:
                        return result
                    if isinstance(bool_op, ast.Or) and result:
                        return result
                return result
            case ast.IfExp(test=test, body=body, orelse=orelse):
                return self._eval(body if self._eval(test, values) else orelse, values)
            case ast.Subscript(value=value, slice=index):
                container, key = self._eval(value, values), self._eval(index, values)
                if not isinstance(container, (Mapping, list, tuple)):
                    raise self._reject(node, "indexing into something other than a list or an object")
                return container[key]
            case ast.List(elts=elts) | ast.Tuple(elts=elts):
                return [self._eval(n, values) for n in elts]
            case ast.Call(func=ast.Name(id=fname), args=args):
                return FUNCTIONS[fname](*(self._eval(n, values) for n in args))
        raise self._reject(node)  # unreachable for a checked tree
