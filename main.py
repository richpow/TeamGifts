import os
import time
import psycopg2
import psycopg2.extras
import requests

DATABASE_URL = os.getenv("DATABASE_URL")
TEAM_WEBHOOK_URL = os.getenv("TEAM_WEBHOOK_URL")

IMAGE_BASE = "https://raw.githubusercontent.com/richpow/tiktok-live-listener/main/gifts"

MANAGER_EMAILS = {
    "rich.powell@hotmail.com",
    "haldane007@icloud.com",
    "allan.campbell3@icloud.com",
    "mitchellcolby008@gmail.com",
    "jeffreyadams6767@gmail.com"
}

GIFT_THRESHOLD = int(os.getenv("GIFT_THRESHOLD", "200"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

NO_MATCH_LOG_EVERY_SECONDS = int(os.getenv("NO_MATCH_LOG_EVERY_SECONDS", "600"))


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def fetch_recent_gifts(since_ts):
    sql = """
        select
            g.creator_username,
            g.sender_username,
            g.sender_display_name,
            g.gift_name,
            g.diamonds_per_item,
            g.repeat_count,
            g.total_diamonds,
            g.received_at
        from fasttrack_live_gifts g
        join users u
            on u.tiktok_username = g.creator_username
        where u.creator_network_manager = any(%s)
          and g.total_diamonds >= %s
          and g.received_at > %s
        order by g.received_at asc
    """

    conn = None
    cur = None
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, (list(MANAGER_EMAILS), GIFT_THRESHOLD, since_ts))
        return cur.fetchall()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def build_image_url(gift_name: str):
    key = (
        (gift_name or "")
        .lower()
        .strip()
        .replace(" ", "_")
        .replace("'", "")
        .replace(".", "")
    )
    return f"{IMAGE_BASE}/{key}.png?raw=true"


def send_team_alert(row):
    if not TEAM_WEBHOOK_URL:
        print("TEAM_WEBHOOK_URL is missing, cannot send Discord alerts")
        return

    creator = row["creator_username"]
    sender_user = row["sender_username"]
    gift = row["gift_name"]
    diamonds = row["diamonds_per_item"]
    total = row["total_diamonds"]

    image_url = build_image_url(gift)

    embed = {
        "title": "Gift Alert",
        "description": f"**{creator}** has just received a **{gift}** from **{sender_user}**.",
        "color": 3447003,
        "thumbnail": {"url": image_url},
        "fields": [
            {"name": "Diamonds", "value": f"{int(diamonds or 0):,}", "inline": False},
            {"name": "Total", "value": f"{int(total or 0):,}", "inline": False},
        ],
    }

    try:
        r = requests.post(TEAM_WEBHOOK_URL, json={"embeds": [embed]}, timeout=10)
        if r.status_code >= 300:
            print(f"Discord returned {r.status_code}: {r.text[:300]}")
        else:
            print(f"[SENT] {creator} received {gift} ({int(total or 0):,} diamonds)")
    except Exception as e:
        print("Discord request failed:", e)


def main_loop():
    print("Team gift poller started…")
    print(f"Using GIFT_THRESHOLD={GIFT_THRESHOLD}, POLL_SECONDS={POLL_SECONDS}")

    last_seen = time.time() - 120
    last_no_match_log = 0.0

    while True:
        try:
            since_ts = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(last_seen))
            rows = fetch_recent_gifts(since_ts=since_ts)
        except Exception as e:
            print("DB fetch failed:", e)
            time.sleep(POLL_SECONDS)
            continue

        if not rows:
            now = time.time()
            if now - last_no_match_log >= NO_MATCH_LOG_EVERY_SECONDS:
                print("No matching gifts in this poll window")
                last_no_match_log = now
        else:
            for row in rows:
                send_team_alert(row)
                received_at = row["received_at"]
                try:
                    last_seen = max(last_seen, received_at.timestamp())
                except Exception:
                    pass

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main_loop()
