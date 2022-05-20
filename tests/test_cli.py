import pytest
from click.testing import CliRunner

from eopf.cli import eopf_cli


@pytest.mark.unit
@pytest.mark.parametrize("opts", [[], ("--help",)])
def test_cli(opts):
    runner = CliRunner()
    r = runner.invoke(eopf_cli, args=opts)
    assert r.exit_code == 0
    assert (
        r.output
        == """Usage: eopf [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  kafka-consumer  Get and load messages from kafka an execute EOTrigger
  trigger         CLI commands to trigger EOProcessingUnit
  web-server      Run web server to run EOTrigger with post payload
"""
    )
