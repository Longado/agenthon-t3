"""Wrap every statement-level `logger.debug(...)` / `logger.info(...)` call in an isEnabledFor guard.

The sims run at WARNING, so these calls never emit, but their f-string arguments are formatted
eagerly (Order.__str__, fmt_ts, whole open-order dicts). The guard skips that work and changes no
emitted output. Prints every argument that contains a call so side effects can be reviewed.

    python tools/guard_logs.py abides/**/*.py
"""

from __future__ import annotations

import ast
import sys

LEVELS = {"debug": 10, "info": 20}


def rewrite(path: str) -> int:
    src = open(path).read()
    lines = src.splitlines(keepends=True)
    targets = []
    for node in ast.walk(ast.parse(src)):
        if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)):
            continue
        f = node.value.func
        if not (isinstance(f, ast.Attribute) and f.attr in LEVELS
                and isinstance(f.value, ast.Name) and f.value.id == "logger"):
            continue
        indent = lines[node.lineno - 1][: node.col_offset]
        assert indent.strip() == "", f"{path}:{node.lineno} shares its line with other code"
        assert lines[node.end_lineno - 1].rstrip().endswith(")"), f"{path}:{node.end_lineno}"
        for arg in node.value.args:
            for sub in ast.walk(arg):
                if isinstance(sub, ast.Call):
                    print(f"  review {path}:{node.lineno}: {ast.unparse(sub)[:90]}")
                    break
        targets.append((node.lineno, node.end_lineno, indent, LEVELS[f.attr]))
    for start, end, indent, level in sorted(targets, reverse=True):
        body = ["    " + ln if ln.strip() else ln for ln in lines[start - 1:end]]
        lines[start - 1:end] = [f"{indent}if logger.isEnabledFor({level}):\n", *body]
    if targets:
        out = "".join(lines)
        ast.parse(out)  # must still be valid Python
        open(path, "w").write(out)
    return len(targets)


if __name__ == "__main__":
    total = sum(rewrite(p) for p in sys.argv[1:])
    print(f"guarded {total} calls in {len(sys.argv) - 1} files")
