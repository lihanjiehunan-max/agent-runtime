import json
import sys
from collections.abc import Sequence


def worker_health() -> dict[str, str]:
    return {"service": "runtime_worker", "status": "live"}


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv) if argv is not None else sys.argv[1:]
    if arguments != ["health"]:
        print("usage: python -m apps.runtime_worker.main health", file=sys.stderr)
        return 2

    print(json.dumps(worker_health(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
