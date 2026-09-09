"""A tiny REPL over :class:`TinyKV`.

    python -m tinykv mydb/
    tinykv> set user:1 alice
    tinykv> get user:1
    alice
    tinykv> scan user:
    user:1 = alice
    tinykv> stats
"""

from __future__ import annotations

import shlex
import sys

from .db import TinyKV

HELP = """commands:
  set <key> <value>     store a pair
  get <key>             print a value
  del <key>             delete a key
  scan [prefix] [n]     list pairs (optionally by prefix, limit n)
  stats                 storage engine statistics
  help                  this text
  exit                  quit
"""


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        print("usage: python -m tinykv <directory>", file=sys.stderr)
        return 2
    db = TinyKV(argv[0])
    print(f"TinyKV — {argv[0]}  ({len(db)} keys). type 'help'.")
    try:
        while True:
            try:
                line = input("tinykv> ").strip()
            except EOFError:
                print()
                break
            if not line:
                continue
            try:
                parts = shlex.split(line)
            except ValueError as e:
                print(f"parse error: {e}")
                continue
            cmd, args = parts[0].lower(), parts[1:]
            if cmd in ("exit", "quit"):
                break
            elif cmd == "help":
                print(HELP)
            elif cmd == "set" and len(args) >= 2:
                db.set(args[0], " ".join(args[1:]))
            elif cmd == "get" and len(args) == 1:
                v = db.get(args[0])
                print(v if v is not None else "(nil)")
            elif cmd == "del" and len(args) == 1:
                print("(deleted)" if db.delete(args[0]) else "(nil)")
            elif cmd == "scan":
                prefix = args[0] if args else ""
                limit = int(args[1]) if len(args) > 1 else None
                for k, v in db.scan(prefix, limit):
                    print(f"{k} = {v}")
            elif cmd == "stats":
                for k, v in db.stats().items():
                    print(f"{k:16} {v}")
            else:
                print("?  type 'help'")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
