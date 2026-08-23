#!/usr/bin/env python3
import argparse
import json
import os
from collections.abc import Iterable, Iterator, Sequence

import httpx


APPROVED_TURNS = (
    "请记住，我叫 Herry，负责散运业务。",
    "我叫什么名字，负责什么业务？",
    "分析航运经营收入时应该关注哪些维度？没有数据的地方不要编造。",
)


def parse_sse_lines(lines: Iterable[str]) -> Iterator[tuple[str, dict]]:
    event_name: str | None = None
    data: dict | None = None
    for line in lines:
        if not line:
            if event_name is not None and data is not None:
                yield event_name, data
            event_name = None
            data = None
            continue
        if line.startswith("event: "):
            event_name = line.removeprefix("event: ")
        elif line.startswith("data: "):
            data = json.loads(line.removeprefix("data: "))
    if event_name is not None and data is not None:
        yield event_name, data


def validate_turns(
    turns: Sequence[Sequence[tuple[str, dict]]],
    *,
    secrets_to_scan: Sequence[str],
) -> None:
    assert len(turns) == 3, "expected exactly three turns"
    answers: list[str] = []
    for events in turns:
        names = [name for name, _ in events]
        assert "delta" in names and names[-1] == "done", (
            "each turn must contain delta before done"
        )
        assert names.count("done") == 1, "each turn must contain exactly one done"
        assert "error" not in names, "a successful turn must not contain error"
        answers.append(
            "".join(
                data.get("content", "")
                for name, data in events
                if name == "delta"
            )
        )

    assert "Herry" in answers[1] and "散运" in answers[1], (
        "turn two did not preserve the requested Session facts"
    )
    assert all(section in answers[2] for section in ("结论", "依据", "建议")), (
        "turn three did not follow the Skill response structure"
    )

    serialized = json.dumps(turns, ensure_ascii=False, separators=(",", ":"))
    for secret in secrets_to_scan:
        if secret:
            assert secret not in serialized, "a configured secret appeared in output"


def validate_service(base_url: str) -> None:
    service_key = os.getenv("SERVICE_API_KEY")
    if not service_key:
        raise SystemExit("SERVICE_API_KEY is required")
    headers = {"Authorization": f"Bearer {service_key}"}
    timeout = httpx.Timeout(130.0, connect=10.0)

    with httpx.Client(
        base_url=base_url.rstrip("/"),
        headers=headers,
        timeout=timeout,
    ) as client:
        deployed = client.post(
            "/api/v1/runtime/ops/agents/shipping-analyst/deploy"
        )
        deployed.raise_for_status()
        created = client.post(
            "/api/v1/runtime/agents/shipping-analyst/sessions"
        )
        created.raise_for_status()
        session_id = created.json()["session_id"]

        turns = []
        for message in APPROVED_TURNS:
            with client.stream(
                "POST",
                f"/api/v1/runtime/sessions/{session_id}/chat",
                json={"message": message},
            ) as response:
                response.raise_for_status()
                events = list(parse_sse_lines(response.iter_lines()))
            turns.append(events)
            answer = "".join(
                data.get("content", "")
                for name, data in events
                if name == "delta"
            )
            print(answer, flush=True)

    validate_turns(
        turns,
        secrets_to_scan=tuple(
            value
            for value in (service_key, os.getenv("MODEL_API_KEY"))
            if value
        ),
    )
    print("Validation PASS", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate the external Session SSE service"
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
    )
    args = parser.parse_args()
    validate_service(args.base_url)


if __name__ == "__main__":
    main()
