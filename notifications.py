import requests
import time
import re
import os
from requests.auth import HTTPBasicAuth

# 🔔 ntfy.sh Configuration
NTFY_TOPIC = "bot_xrp"
NTFY_USERNAME = "botuser"
NTFY_PASSWORD = "botuser"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

# 📄 Log file path

LOG_FILE = "/home/rasp/bot/bot_xrp_v6_unified.log"

ROUND_PATTERN = re.compile(r'\| ROUND \| PnL: \$([+-])')
TOTAL_PATTERN = re.compile(r'\| TOTAL \| PnL')


def send_ntfy_notification(message, title="", tags=""):
    """Send a notification to ntfy.sh with authentication."""
    headers = {}

    # IMPORTANT: Headers must be latin-1 compatible (NO emojis here)
    if title:
        headers["Title"] = title
    if tags:
        headers["Tags"] = tags

    try:
        response = requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),  # Body CAN contain emojis
            auth=HTTPBasicAuth(NTFY_USERNAME, NTFY_PASSWORD),
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            print("✅ ntfy.sh alert sent successfully!")
        else:
            print(f"❌ Failed to send ntfy.sh alert: {response.status_code} - {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error while sending notification: {e}")


def monitor_log():
    """Tail the log file and send a notification whenever a ROUND + TOTAL PnL block is detected."""
    print(f"📄 Starting log monitor on {LOG_FILE} ...")

    with open(LOG_FILE, 'r') as f:
        # Move to end of file
        f.seek(0, os.SEEK_END)

        pending_round_line = None
        pending_is_positive = None

        while True:
            line = f.readline()

            if not line:
                time.sleep(0.5)
                continue

            line = line.rstrip('\n')

            round_match = ROUND_PATTERN.search(line)
            if round_match:
                pending_round_line = line
                pending_is_positive = (round_match.group(1) == '+')
                continue

            if pending_round_line and TOTAL_PATTERN.search(line):
                total_line = line

                if pending_is_positive:
                    title = "Positive Round"
                    tags = "white_check_mark,chart_with_upwards_trend"
                    message = f"✅ {pending_round_line}\n📊 {total_line}"
                    print("📈 Positive PnL detected → sending notification")
                else:
                    title = "Negative Round"
                    tags = "red_circle,chart_with_downwards_trend"
                    message = f"🔴 {pending_round_line}\n📊 {total_line}"
                    print("📉 Negative PnL detected → sending notification")

                send_ntfy_notification(message, title=title, tags=tags)

                pending_round_line = None
                pending_is_positive = None


if __name__ == "__main__":
    try:
        monitor_log()
    except KeyboardInterrupt:
        print("\n🛑 Log monitor stopped by user.")