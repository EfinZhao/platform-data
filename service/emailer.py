import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL


def _fmt_dt(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        return datetime.fromisoformat(iso).strftime("%b %d, %Y %I:%M %p")
    except ValueError:
        return iso


def _status_badge(status) -> str:
    if status == 1:
        return (
            '<span style="display:inline-block;background:#dcfce7;'
            'color:#166534;border-radius:9999px;padding:2px 10px;font-size:11px;font-weight:500;">'
            'Online</span>'
        )
    if status == 0:
        return (
            '<span style="display:inline-block;background:#fee2e2;'
            'color:#991b1b;border-radius:9999px;padding:2px 10px;font-size:11px;font-weight:500;">'
            'Offline</span>'
        )
    return (
        '<span style="display:inline-block;background:#f3f4f6;'
        'color:#4b5563;border-radius:9999px;padding:2px 10px;font-size:11px;font-weight:500;">'
        'Unknown</span>'
    )


def _api_dot(available) -> str:
    if available == 1:
        return '<span style="color:#22c55e;font-size:13px;line-height:1;" title="Available">✓</span>'
    if available == 0:
        return '<span style="color:#ef4444;font-size:13px;line-height:1;" title="Unavailable">✗</span>'
    return '<span style="color:#d1d5db;font-size:13px;line-height:1;" title="Unknown">●</span>'


def _sensor_table(rows, include_api: bool) -> str:
    TH = (
        'style="padding:10px 14px;text-align:left;font-size:10px;font-weight:600;'
        'color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;'
        'background:#f9fafb;border-bottom:1px solid #e5e7eb;"'
    )
    TH_C = (
        'style="padding:10px 14px;text-align:center;font-size:10px;font-weight:600;'
        'color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;'
        'background:#f9fafb;border-bottom:1px solid #e5e7eb;"'
    )

    api_headers = f'<th {TH_C}>Frame</th><th {TH_C}>Phase</th><th {TH_C}>TTC</th>' if include_api else ''

    body = ""
    for i, r in enumerate(rows):
        row_bg = "#f9fafb" if i % 2 else "white"
        api_cells = (
            f'<td style="padding:10px 14px;text-align:center;border-bottom:1px solid #f3f4f6;">{_api_dot(r["frame_status"])}</td>'
            f'<td style="padding:10px 14px;text-align:center;border-bottom:1px solid #f3f4f6;">{_api_dot(r["phase_status"])}</td>'
            f'<td style="padding:10px 14px;text-align:center;border-bottom:1px solid #f3f4f6;">{_api_dot(r["ttc_status"])}</td>'
        ) if include_api else ''
        body += (
            f'<tr style="background:{row_bg};">'
            f'<td style="padding:10px 14px;color:#111827;font-size:13px;font-family:monospace;border-bottom:1px solid #f3f4f6;">{r["udid"]}</td>'
            f'<td style="padding:10px 14px;border-bottom:1px solid #f3f4f6;">{_status_badge(r["status"])}</td>'
            f'{api_cells}'
            f'<td style="padding:10px 14px;color:#6b7280;font-size:12px;border-bottom:1px solid #f3f4f6;white-space:nowrap;">{_fmt_dt(r["last_online"])}</td>'
            f'</tr>'
        )

    return (
        f'<div style="background:white;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,0.04);">'
        f'<table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;font-size:13px;">'
        f'<thead><tr><th {TH}>UDID</th><th {TH}>Status</th>{api_headers}<th {TH}>Last Online</th></tr></thead>'
        f'<tbody>{body}</tbody>'
        f'</table></div>'
    )


def _build_html(rows) -> str:
    now = datetime.now()

    online_rows  = [r for r in rows if r["status"] == 1]
    offline_rows = [r for r in rows if r["status"] != 1]

    online_section = (
        f'<h2 style="margin:0 0 10px;font-size:15px;font-weight:600;color:#111827;">Online Sensors</h2>'
        + (_sensor_table(online_rows, include_api=True) if online_rows else
           '<p style="color:#6b7280;font-size:13px;">No sensors online.</p>')
    )

    offline_section = (
        f'<h2 style="margin:24px 0 10px;font-size:15px;font-weight:600;color:#111827;">Offline / Unknown Sensors</h2>'
        + (_sensor_table(offline_rows, include_api=False) if offline_rows else
           '<p style="color:#6b7280;font-size:13px;">No sensors offline.</p>')
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>BCT Sensor Report</title>
</head>
<body style="margin:0;padding:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:740px;margin:32px auto;padding:0 16px;">

  <!-- Title -->
  <div style="margin-bottom:20px;">
    <h1 style="margin:0;font-size:22px;font-weight:700;color:#111827;letter-spacing:-0.02em;">BCT Sensor Dashboard</h1>
    <p style="margin:4px 0 0;font-size:13px;color:#6b7280;">Daily report &mdash; {now.strftime("%B %d, %Y")}</p>
  </div>

  {online_section}
  {offline_section}

  <p style="text-align:center;font-size:11px;color:#9ca3af;margin:20px 0 32px;">
    BCT Dashboard &mdash; Generated {now.strftime("%Y-%m-%d %H:%M:%S")}
  </p>

</div>
</body>
</html>"""


def _change_section(title: str, rows: list, include_service: bool) -> str:
    if not rows:
        return ""

    TH = (
        'style="padding:10px 14px;text-align:left;font-size:10px;font-weight:600;'
        'color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;'
        'background:#f9fafb;border-bottom:1px solid #e5e7eb;"'
    )
    service_header = f"<th {TH}>Service</th>" if include_service else ""

    body = ""
    for i, r in enumerate(rows):
        row_bg = "#f9fafb" if i % 2 else "white"
        service_cell = (
            f'<td style="padding:10px 14px;color:#374151;font-size:13px;'
            f'border-bottom:1px solid #f3f4f6;">{r["service"]}</td>'
        ) if include_service else ""
        body += (
            f'<tr style="background:{row_bg};">'
            f'<td style="padding:10px 14px;color:#111827;font-size:13px;'
            f'font-family:monospace;border-bottom:1px solid #f3f4f6;">{r["udid"]}</td>'
            f'<td style="padding:10px 14px;color:#374151;font-size:13px;'
            f'border-bottom:1px solid #f3f4f6;">{r["name"]}</td>'
            f"{service_cell}"
            f"</tr>"
        )

    return (
        f'<h2 style="margin:24px 0 10px;font-size:15px;font-weight:600;color:#111827;">'
        f"{title}</h2>"
        f'<div style="background:white;border:1px solid #e5e7eb;border-radius:8px;'
        f'overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,0.04);">'
        f'<table width="100%" cellspacing="0" cellpadding="0"'
        f' style="border-collapse:collapse;font-size:13px;">'
        f"<thead><tr>"
        f"<th {TH}>UDID</th>"
        f"<th {TH}>Sensor</th>"
        f"{service_header}"
        f"</tr></thead>"
        f"<tbody>{body}</tbody>"
        f"</table></div>"
    )


def _build_alert_html(changes: dict) -> str:
    now = datetime.now()

    sections = (
        _change_section("Sensor Online", changes.get("sensor_up", []), include_service=False)
        + _change_section("Sensor Offline", changes.get("sensor_down", []), include_service=False)
        + _change_section("Service Online", changes.get("service_up", []), include_service=True)
        + _change_section("Service Offline", changes.get("service_down", []), include_service=True)
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>BCT Sensor Alert</title>
</head>
<body style="margin:0;padding:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:740px;margin:32px auto;padding:0 16px;">

  <div style="margin-bottom:20px;">
    <h1 style="margin:0;font-size:22px;font-weight:700;color:#111827;letter-spacing:-0.02em;">BCT Sensor Alert</h1>
    <p style="margin:4px 0 0;font-size:13px;color:#6b7280;">Status change detected &mdash; {now.strftime("%B %d, %Y %I:%M %p")}</p>
  </div>

  {sections}

  <p style="text-align:center;font-size:11px;color:#9ca3af;margin:20px 0 32px;">
    BCT Dashboard &mdash; Generated {now.strftime("%Y-%m-%d %H:%M:%S")}
  </p>

</div>
</body>
</html>"""


def send_change_alert(changes: dict) -> None:
    parts = []
    if changes.get("sensor_down"):
        n = len(changes["sensor_down"])
        parts.append(f"{n} sensor{'s' if n > 1 else ''} offline")
    if changes.get("sensor_up"):
        n = len(changes["sensor_up"])
        parts.append(f"{n} sensor{'s' if n > 1 else ''} online")
    if changes.get("service_down"):
        n = len(changes["service_down"])
        parts.append(f"{n} service{'s' if n > 1 else ''} offline")
    if changes.get("service_up"):
        n = len(changes["service_up"])
        parts.append(f"{n} service{'s' if n > 1 else ''} online")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"BCT Alert — {', '.join(parts)}"
    msg["From"] = SENDER_EMAIL  # type: ignore
    msg["To"] = RECEIVER_EMAIL  # type: ignore
    msg.attach(MIMEText(_build_alert_html(changes), "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)  # type: ignore
        smtp.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())  # type: ignore


def send_daily_report(rows) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"BCT Sensor Report — {datetime.now().strftime('%B %d, %Y')}"
    msg["From"]    = SENDER_EMAIL  # type: ignore
    msg["To"]      = RECEIVER_EMAIL  # type: ignore
    msg.attach(MIMEText(_build_html(rows), "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)  # type: ignore
        smtp.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())  # type: ignore
