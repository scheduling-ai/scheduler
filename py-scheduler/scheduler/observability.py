"""Logging and Sentry setup. Import this module to initialise."""

import logging
import os


def init() -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=logging.INFO,
    )

    sentry_dsn = os.environ.get("SENTRY_DSN")
    if not sentry_dsn:
        return

    import sentry_sdk

    sentry_sdk.init(
        dsn=sentry_dsn,
        release=os.environ.get("GIT_SHA", "dev"),
        environment=os.environ.get("SENTRY_ENV", "production"),
        traces_sample_rate=1.0,
    )


init()
