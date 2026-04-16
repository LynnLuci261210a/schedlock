"""Command-line interface for inspecting and managing schedlock locks."""

import argparse
import sys
from schedlock.backends.file_backend import FileBackend
from schedlock.utils import make_lock_key


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="schedlock",
        description="Inspect and manage distributed cron-style locks.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- status ---
    status_parser = subparsers.add_parser("status", help="Check lock status for a job.")
    status_parser.add_argument("job_name", help="Name of the job to check.")
    status_parser.add_argument(
        "--prefix", default="schedlock", help="Lock key prefix (default: schedlock)."
    )
    status_parser.add_argument(
        "--lock-dir", default="/tmp/schedlock", help="Directory for file-based locks."
    )

    # --- release ---
    release_parser = subparsers.add_parser("release", help="Force-release a lock.")
    release_parser.add_argument("job_name", help="Name of the job whose lock to release.")
    release_parser.add_argument("--owner", required=True, help="Owner ID that holds the lock.")
    release_parser.add_argument(
        "--prefix", default="schedlock", help="Lock key prefix (default: schedlock)."
    )
    release_parser.add_argument(
        "--lock-dir", default="/tmp/schedlock", help="Directory for file-based locks."
    )

    return parser


def cmd_status(args) -> int:
    backend = FileBackend(lock_dir=args.lock_dir)
    key = make_lock_key(args.job_name, prefix=args.prefix)
    locked = backend.is_locked(key)
    state = "LOCKED" if locked else "FREE"
    print(f"[{state}] {key}")
    return 0


def cmd_release(args) -> int:
    backend = FileBackend(lock_dir=args.lock_dir)
    key = make_lock_key(args.job_name, prefix=args.prefix)
    released = backend.release(key, owner=args.owner)
    if released:
        print(f"Released lock: {key}")
        return 0
    else:
        print(f"Could not release lock (not held by owner '{args.owner}'): {key}", file=sys.stderr)
        return 1


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "status":
        return cmd_status(args)
    elif args.command == "release":
        return cmd_release(args)
    else:
        parser.print_help()
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
