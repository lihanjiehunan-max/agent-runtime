from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONSOLE = ROOT / "apps" / "runtime_console"


def read(relative: str) -> str:
    return (CONSOLE / relative).read_text(encoding="utf-8")


def test_console_is_a_fresh_app_and_does_not_import_legacy_public_console() -> None:
    package = read("package.json")
    app = read("src/App.tsx")
    assert '"vite"' in package
    assert '"react"' in package
    assert "public/app.js" not in app
    assert "localStorage" not in app
    assert "console.log" not in app


def test_console_has_five_views_and_explicit_provenance() -> None:
    app = read("src/App.tsx")
    badge = read("src/components/ModeBadge.tsx")
    for label in ("Agents", "Sessions", "Chat", "Trace", "Dashboard"):
        assert label in app
    assert '"LIVE"' in app
    assert '"DEMO"' in app
    assert "No live result was generated" in app
    assert "Manual DEMO" in app
    assert 'data-testid="mode-badge"' in badge


def test_runtime_client_uses_memory_auth_and_resumable_sse_contract() -> None:
    client = read("src/api/runtimeClient.ts")
    assert '"Authorization"' in client
    assert "Bearer ${this.token}" in client
    assert "Last-Event-ID" in client
    assert "after_sequence" in client
    assert "cancelTask" in client
    assert "localStorage" not in client
    assert "console.log" not in client
    assert "sanitizeErrorText" in client


def test_same_origin_default_and_vite_proxy_are_explicit() -> None:
    types = read("src/types/runtime.ts")
    vite = read("vite.config.ts")
    assert '"/api/v1/runtime"' in types
    assert '"/api/v1/runtime"' in vite
    assert "VITE_RUNTIME_PROXY_TARGET" in vite
    assert "127.0.0.1" in vite


def test_acceptance_surface_has_required_client_tests() -> None:
    client_test = read("src/api/runtimeClient.test.ts")
    app_test = read("src/App.test.tsx")
    for marker in ("Last-Event-ID", "secret-token-value", "cancelTask", "parses SSE"):
        assert marker in client_test
    for marker in ("same Session", "Use DEMO", "five operational views"):
        assert marker in app_test


def test_console_matches_flat_execution_and_honest_live_controls() -> None:
    app = read("src/App.tsx")
    types = read("src/types/runtime.ts")
    chat = read("src/pages/ChatPage.tsx")
    assert "result.status" in app
    assert "result.execution.status" not in app
    assert 'event.event_type === "model.delta"' in app
    assert "messages" in app
    assert "resetSessionState" in app
    assert "RuntimeExecutionResult" in types
    assert "output: unknown | null" in types
    assert "Stop browser wait" in chat
    assert "Runtime execution may continue" in chat
