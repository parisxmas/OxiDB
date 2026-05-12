"""
Rotate an admin user's username and/or password.

Embedded OxiDB is single-process, so this command can't run while
gunicorn is up — they'd be two writers to the same data dir. Run
the rotation via `docker compose run --rm blog python manage.py
rotate_admin …` (which boots a one-shot container instead of joining
the running one), then bring the live service back up.

Usage:
    manage.py rotate_admin --old-username admin --username NEWUSER --password NEWPW
    manage.py rotate_admin --old-username admin --username NEWUSER  # prompts for password

The destination user must not already exist — collision is an error
(refuse to overwrite somebody else's account).
"""
import getpass

from django.core.management.base import BaseCommand, CommandError

from posts import db


class Command(BaseCommand):
    help = "Rotate the admin user (username + password)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--old-username", default="admin",
            help="The existing admin user to rotate. Defaults to 'admin'.",
        )
        parser.add_argument(
            "--username", required=True,
            help="The new username.",
        )
        parser.add_argument(
            "--password", default=None,
            help="The new password. Prompted interactively if omitted.",
        )

    def handle(self, *args, **opts):
        old = opts["old_username"]
        new_user = opts["username"].strip()
        new_pw = opts["password"]

        if not new_user:
            raise CommandError("Empty new username.")
        if new_pw is None:
            new_pw = getpass.getpass("New password: ")
            confirm = getpass.getpass("Confirm:      ")
            if new_pw != confirm:
                raise CommandError("Passwords don't match.")
        if len(new_pw) < 8:
            raise CommandError("Password too short (need ≥ 8 chars).")

        d = db.db()
        existing = d.find_one(db.ADMINS, {"username": old})
        if not existing:
            raise CommandError(f"User '{old}' not found.")

        # If the new username differs from the old one, make sure the
        # destination slot is free.
        if new_user != old and d.find_one(db.ADMINS, {"username": new_user}):
            raise CommandError(
                f"A different user already owns username '{new_user}'. "
                "Refusing to overwrite."
            )

        d.update_one(
            db.ADMINS,
            {"_id": existing["_id"]},
            {"$set": {
                "username": new_user,
                "password_hash": db.hash_password(new_pw),
            }},
        )
        self.stdout.write(self.style.SUCCESS(
            f"rotated: {old} → {new_user}  (id={existing['_id']})"
        ))
