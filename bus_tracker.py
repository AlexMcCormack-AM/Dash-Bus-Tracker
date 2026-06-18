import requests
import time
import smtplib
from email.mime.text import MIMEText
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv
import os

load_dotenv()

# Config
API_KEY = os.getenv("SWIFTLY_API_KEY", "4af18b965e8a21f6015686f2f208f95f")
STOP_ID = "413"  # S Royal St + Duke St
STOP_NAME = "S Royal St + Duke St"
ALERT_MINUTES = 10        # text when bus is this many minutes away
POLL_INTERVAL = 60        # check every 60 seconds
SMS_GATEWAY = os.getenv("SMS_GATEWAY", "5408099248@vtext.com")

# Gmail config — fill these in .env
GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")


def get_upcoming_buses():
    """Returns list of (route_id, minutes_away) sorted by arrival."""
    r = requests.get(
        "https://api.goswift.ly/real-time/alexandria-dash/gtfs-rt-trip-updates",
        headers={"Authorization": API_KEY},
        timeout=10,
    )
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)

    now = int(time.time())
    buses = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            for stu in entity.trip_update.stop_time_update:
                if stu.stop_id == STOP_ID:
                    arrival = stu.arrival.time if stu.HasField("arrival") else None
                    if arrival and arrival > now:
                        mins = round((arrival - now) / 60, 1)
                        buses.append((entity.trip_update.trip.route_id, mins))

    return sorted(buses, key=lambda x: x[1])


def send_text(message):
    """Send SMS via email-to-text gateway using Gmail."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print(f"[SMS would send]: {message}")
        return

    msg = MIMEText(message)
    msg["From"] = GMAIL_USER
    msg["To"] = SMS_GATEWAY
    msg["Subject"] = ""

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.send_message(msg)
    print(f"Text sent: {message}")


def main():
    print(f"Watching stop: {STOP_NAME}")
    print(f"Will alert when bus is within {ALERT_MINUTES} minutes")
    print(f"Checking every {POLL_INTERVAL} seconds...\n")

    alerted = set()  # track which (route, arrival_bucket) we've already texted

    while True:
        try:
            buses = get_upcoming_buses()

            for route, mins in buses:
                print(f"  Route {route}: {mins} min away")
                # Alert key: route + 5-min arrival bucket to avoid duplicate texts
                alert_key = (route, int(mins // 5))

                if mins <= ALERT_MINUTES and alert_key not in alerted:
                    message = f"Route {route} is {int(mins)} min away at {STOP_NAME}"
                    send_text(message)
                    alerted.add(alert_key)

            # Clear old alert keys every hour so they reset
            if len(alerted) > 50:
                alerted.clear()

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
