from dotenv import load_dotenv
import os

load_dotenv()

BCT_USERNAME = os.getenv("BCT_API_USERNAME")
BCT_PASSWORD = os.getenv("BCT_API_password")
BCT_BEACON_ADDRESS = "https://realtime.us.beacon.1.api.bluecity.ai/"

SENDER_EMAIL    = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECEIVER_EMAIL  = os.getenv("RECIEVER_EMAIL")  # typo preserved from .env
