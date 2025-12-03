import os
import time
import psycopg2
import requests

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
GROUP_LABEL = os.getenv("GROUP_LABEL")

# Threshold for alerts
ALERT_THRESHOLD = 1000

# Poll every 20 seconds
POLL_INTERVAL = 20

# Track the last processed row
last_id = 0


def get_db():
    return psycopg2.connect(DATABASE_URL)


def fetch_creators():
    """
    Finds all creators in the user's team by assigned_group.
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        select tiktok_username
        from users
        where assigned_group = %s
          and tiktok_username is not null
          and tiktok_username <> ''
          and creator_id is not null
          and creator_id <> ''
        """,
        (GROUP_LABEL,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


def fetch_new_gifts():
    """
    Returns new gifts from fasttrack_team_gifts with ID > last_id
    Only returns those over the alert threshold AND belonging to the team.
    """
    global last_id

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        select g.id,
               g.creator_username,
               g.sender_username,
               g.sender_display_name,
               g.gift_name,
               g.diamonds_per_item,
               g.repeat_count,
               g.total_diamonds,
               g.received_at
        from fasttrack_team_gifts g
        join users u
          on u.tiktok_username = g.creator_username
        where g.id > %s
          and u.assigned_group = %s
          and g.total_diamonds >= %s
        order by g.id asc
        """,
        (last_id, GROUP_LABEL, ALERT_THRESHOLD),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Update last ID if any rows
    if rows:
        last_id = rows[-1][0]

    return rows


def send_discord_alert(
    creator_username,
    sender_username,
    sender_display_name,
    gift_name,
    diamonds_per_item,
    repeat_count,
    total_diamonds,
    received_at,
):
    """
    Sends a styled Discord embed.
    """

    embed = {
        "title": "Gift Alert",
        "description": f"**{creator_username}** has just received a **{gift_name}**",
        "color": 3447003,  # Blue bar
        "fields": [
            {"name": "Creator", "value": creator_username, "inline": False},
            {"name": "From", "value": sender_username, "inline": False},
            {"name": "Display Name", "value": sender_display_name, "inline": False},
            {"name": "Gift", "value": gift_name, "inline": False},
            {"name": "Diamonds", "value": str(diamonds_per_item), "inline": True},
            {"name": "Count", "value": str(repeat_count), "inline": True},
            {"name": "Total Diamonds", "value": str(total_diamonds), "inline": False},
            {"name": "Received At", "value": str(received_at), "inline": False},
        ],
    }

    payload = {"embeds": [embed]}

    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
        print(f"Alert sent for {creator_username} ({total_diamonds} diamonds)")
    except Exception as e:
        print("Discord webhook error:", e)


def main_loop():
    creators = fetch_creators()
    print(f"Tracking {len(creators)} creators in {GROUP_LABEL}")

    while True:
        try:
            new_gifts = fetch_new_gifts()

            for row in new_gifts:
                (
                    gift_id,
                    creator_username,
                    sender_username,
                    sender_display_name,
                    gift_name,
                    diamonds_per_item,
                    repeat_count,
                    total_diamonds,
                    received_at,
                ) = row

                send_discord_alert(
                    creator_username,
                    sender_username,
                    sender_display_name,
                    gift_name,
                    diamonds_per_item,
                    repeat_count,
                    total_diamonds,
                    received_at,
                )

        except Exception as e:
            print("Error in poll loop:", e)

        # Sleep a bit to avoid Railway rate limits
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    print("Starting Team Gift Monitor...")
    main_loop()
