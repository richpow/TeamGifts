import os
import time
import psycopg2
import requests

DATABASE_URL = os.getenv("DATABASE_URL")
TEAM_WEBHOOK_URL = os.getenv("TEAM_WEBHOOK_URL")

IMAGE_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

GROUP_NAME = "FT (Richard Powell)"
GIFT_THRESHOLD = 400   # minimum diamonds before alert is sent


def db():
    return psycopg2.connect(DATABASE_URL)


def fetch_recent_gifts():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        select
            creator_username,
            sender_username,
            sender_display_name,
            gift_name,
            diamonds_per_item,
            repeat_count,
            total_diamonds
        from fasttrack_live_gifts
        where creator_username in (
            select tiktok_username
            from users
            where assigned_group = %s
              and tiktok_username is not null
              and tiktok_username <> ''
        )
        and total_diamonds >= %s
        and received_at > now() - interval '70 seconds'
        order by received_at desc
    """, (GROUP_NAME, GIFT_THRESHOLD))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def build_image_url(gift_name: str):
    # Convert gift name to GitHub filename format
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

    embed = {
        "title": "Gift Alert",
        "description": f"**{creator}** has just received a **{gift}**",
        "color": 3447003,   # blue accent bar
        "thumbnail": {"url": image_url},
        "fields": [
            {"name": "Creator", "value": creator, "inline": False},
            {"name": "From", "value": sender_user, "inline": False},
            {"name": "Display Name", "value": sender_display, "inline": False},
            {"name": "Gift", "value": gift, "inline": False},
            {"name": "Diamonds", "value": f"{diamonds:,}", "inline": True},
            {"name": "Count", "value": str(count), "inline": True},
            {"name": "Total diamonds", "value": f"{total:,}", "inline": False}
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

        time.sleep(60)  # poll every minute


if __name__ == "__main__":
    main_loop()
