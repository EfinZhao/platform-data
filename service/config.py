import tomllib
from pathlib import Path

# Single shared credentials file — same one logging/logger_rt.py reads.
_SECRETS_PATH = Path(__file__).resolve().parent.parent / "logging" / ".secrets.toml"

with open(_SECRETS_PATH, "rb") as f:
    _secrets = tomllib.load(f)

BCT_USERNAME = _secrets.get("username")
BCT_PASSWORD = _secrets.get("password")
BCT_BEACON_ADDRESS = "https://realtime.us.beacon.1.api.bluecity.ai/"

_email = _secrets.get("email", {})
SENDER_EMAIL    = _email.get("sender")
SENDER_PASSWORD = _email.get("sender_password")
RECEIVER_EMAIL  = _email.get("receiver")
