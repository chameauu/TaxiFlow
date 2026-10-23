from __future__ import annotations

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]
SQLALCHEMY_TRACK_MODIFICATIONS = False
