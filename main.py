import os
import time
import psycopg2
import requests

DATABASE_URL = os.getenv("DATABASE_URL")
TEAM_WEBHOOK_URL = os.getenv("TEAM_WEBHOOK_URL")

IMAGE_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

# Emails that should trigger team alerts
MANAGER_EMAILS = {
    "rich.powell@hotmail.com",
    "haldane007@icloud.com",
    "owensjamie27@gmail.com",
    "allan.campbell3@icloud.com",
    "jamesbcfc97@gmail.com",
    "mitchellcolby008@gmail.com"
}

GIFT_THRESHOLD = 400   # diamonds required to trigger alert


def db():
    return psycopg2.connect(DATABASE_URL)


def fetch_recent_gifts():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        select
            g.creator_username,
            g.sender_username,
            g.sender_display_name,
            g.gift_name,
            g.diamonds_per_item,
            g.repeat_count,
            g.total_diamonds
        from fasttrack_live_gifts g
        join users u
            on u.tiktok_username = g.creator_username
        where u.creator_network_manager = ANY(%s)
          and g.total_diamonds >= %s
          and g.received_at > now() - interval '70 seconds'
        order by g.received_at desc
    """, (list(MANAGER_EMAILS), GIFT_THRESHOLD))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def build_image_url(gift_name: str):
    key = (
        gift_name.lower()
        .strip()
        .replace(" ", "_")
        .replace("'", "")
        .replace(".", "")
    )
    filename = f"{key}.png"
    return f"{IMAGE_BASE}/{filename}?raw=true"


def send_team_alert(row):
    creator, sender_user, sender_display, gift, diamonds, count, total = row

    image_url = build_image_url(gift)

    # Updated embed structure
    embed = {
        "title": "Gift Alert",
        "description": f"**{creator}** has just received a **{gift}** from **{sender_user}**.",
        "color": 3447003,   # blue bar
        "thumbnail": {"url": image_url},
        "fields": [
            {
                "name": "Diamonds",
                "value": f"{diamonds:,}",
                "inline": False
            }
        ]
    }

    payload = {"embeds": [embed]}

    try:
        requests.post(TEAM_WEBHOOK_URL, json=payload)
        print(f"[SENT] {creator} received {gift} ({total} diamonds)")
    except Exception as e:
        print("Discord error:", e)


def main_loop():
    print("Team gift poller started…")

    while True:
        rows = fetch_recent_gifts()

        for row in rows:
            send_team_alert(row)

        time.sleep(60)


if __name__ == "__main__":
    main_loop()
