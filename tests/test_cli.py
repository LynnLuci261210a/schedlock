"""Tests for the schedlock CLI."""

import pytest
from unittest.mock import MagicMock, patch
from schedlock.cli import build_parser, main


@pytest.fixture
def parser():
    return build_parser()


def test_parser_status_defaults(parser):
    args = parser.parse_args(["status", "my_job"])
    assert args.command == "status"
    assert args.job_name == "my_job"
    assert args.prefix == "schedlock"
    assert args.lock_dir == "/tmp/schedlock"


def test_parser_status_custom_prefix(parser):
    args = parser.parse_args(["status", "my_job", "--prefix", "myapp"])
    assert args.prefix == "myapp"


def test_parser_release_requires_owner(parser):
    with pytest.raises(SystemExit):
        parser.parse_args(["release", "my_job"])


def test_parser_release_with_owner(parser):
    args = parser.parse_args(["release", "my_job", "--owner", "host-123"])
    assert args.command == "release"
    assert args.owner == "host-123"


def test_status_locked(capsys):
    with patch("schedlock.cli.FileBackend") as MockBackend:
        instance = MockBackend.return_value
        instance.is_locked.return_value = True
        rc = main(["status", "my_job"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "LOCKED" in out
    assert "my_job" in out


def test_status_free(capsys):
    with patch("schedlock.cli.FileBackend") as MockBackend:
        instance = MockBackend.return_value
        instance.is_locked.return_value = False
        rc = main(["status", "my_job"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "FREE" in out


def test_release_success(capsys):
    with patch("schedlock.cli.FileBackend") as MockBackend:
        instance = MockBackend.return_value
        instance.release.return_value = True
        rc = main(["release", "my_job", "--owner", "host-123"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Released" in out


def test_release_failure(capsys):
    with patch("schedlock.cli.FileBackend") as MockBackend:
        instance = MockBackend.return_value
        instance.release.return_value = False
        rc = main(["release", "my_job", "--owner", "wrong-owner"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "Could not release" in err


def test_release_uses_correct_key():
    with patch("schedlock.cli.FileBackend") as MockBackend:
        instance = MockBackend.return_value
        instance.release.return_value = True
        main(["release", "nightly_report", "--owner", "host-1", "--prefix", "myapp"])
        call_args = instance.release.call_args
        key = call_args[0][0]
        assert "myapp" in key
        assert "nightly_report" in key
