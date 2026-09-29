"""init_logging re-runs in headless mode; loggers created before that must keep logging."""
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_headless_reinit_keeps_controller_loggers_created_at_boot(tmp_path):
    (tmp_path / "conf").mkdir()
    (tmp_path / "logs").mkdir()
    shutil.copy(REPO / "hummingbot" / "templates" / "hummingbot_logs_TEMPLATE.yml",
                tmp_path / "conf" / "hummingbot_logs.yml")
    # A child process: dictConfig replaces process-wide handlers, including pytest's.
    script = textwrap.dedent(f"""
        import logging
        from types import SimpleNamespace
        import hummingbot
        hummingbot.prefix_path = lambda: {str(tmp_path)!r}
        client_config = SimpleNamespace(logger_override_whitelist=[])
        hummingbot.init_logging("hummingbot_logs.yml", client_config)
        controller = logging.getLogger("controllers.directional_trading.reinit_probe")
        hummingbot.init_logging("hummingbot_logs.yml", client_config, strategy_file_path="probe.yml")
        controller.warning("Early stop | probe")
        assert controller.disabled is False
    """)
    subprocess.run([sys.executable, "-c", script], cwd=REPO, check=True, capture_output=True, text=True)
    assert "Early stop | probe" in (tmp_path / "logs" / "logs_probe.log").read_text()
