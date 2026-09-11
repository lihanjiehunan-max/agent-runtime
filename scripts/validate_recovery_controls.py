"""Prove that the final regressions fail against the verified original source.

Run only in a disposable CI checkout. Restores source/build output in finally;
uses synthetic data and never connects a live model or enterprise tool.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation-results'
BASE = '6e439fd3882a8552d5d21abacd216193653613b7'
HARNESS = 'apps/distributed_runtime/harness.py'
CONSOLE = 'apps/runtime_console/src/main.tsx'


def main():
    if os.environ.get('RUN_RECOVERY_NEGATIVE_CONTROL') != '1':
        raise SystemExit('Only run in an isolated checkout with RUN_RECOVERY_NEGATIVE_CONTROL=1')
    OUT.mkdir(exist_ok=True)
    saved = {name: (ROOT/name).read_bytes() for name in (HARNESS, CONSOLE)}
    report = {'status': 'FAIL', 'base_commit': BASE, 'expected_failures': [],
              'scope': 'negative controls only; positive acceptance is a separate step'}

    def run(args, name, cwd=ROOT):
        result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=120)
        (OUT/name).write_text(result.stdout + result.stderr)
        return result

    def restore_baseline(name):
        (ROOT/name).write_bytes(subprocess.check_output(['git', 'show', BASE+':'+name], cwd=ROOT))

    try:
        restore_baseline(HARNESS)
        result = run([sys.executable, '-m', 'pytest',
            'tests/distributed_runtime/test_end_to_end.py::test_actual_harness_large_child_result_is_paged', '-q'],
            'negative-harness.log')
        assert result.returncode == 1 and 'unbounded delegated result' in result.stdout, 'Harness negative control failed for an unexpected reason'
        report['expected_failures'].append('original-harness-has-no-runtime-child-result-paging')
        (ROOT/HARNESS).write_bytes(saved[HARNESS])

        restore_baseline(CONSOLE)
        result = run(['npm', 'run', 'build'], 'negative-console-build.log', ROOT/'apps/runtime_console')
        assert result.returncode == 0, 'Original console build failed'
        result = run([sys.executable, 'scripts/validate_console_browser.py'], 'negative-browser.log')
        browser = json.loads((OUT/'console-browser.json').read_text())
        (OUT/'negative-browser.json').write_text(json.dumps(browser, ensure_ascii=False, indent=2)+'\n')
        assert result.returncode == 1 and 'Session navigation changed an ambiguous submission key' in browser.get('error', ''), 'Browser negative control failed for an unexpected reason'
        report['expected_failures'].append('original-console-loses-key-after-response-loss-and-session-switch')
        report['status'] = 'PASS'
    except Exception as exc:
        report['error'] = str(exc)
    finally:
        for name, data in saved.items():
            (ROOT/name).write_bytes(data)
        result = run(['npm', 'run', 'build'], 'restored-console-build.log', ROOT/'apps/runtime_console')
        if result.returncode:
            report.update(status='FAIL', error='Failed to rebuild restored console')
        report['source_restored'] = all((ROOT/name).read_bytes() == data for name, data in saved.items())
        if not report['source_restored']:
            report.update(status='FAIL', error='Source restoration failed')
        (OUT/'negative-controls.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
