import tomllib

from datetime import datetime, timedelta, timezone

from realtime_subscriber.Realtime_subscriber_api import BCTWSConnection


with open(".secrets.toml", "rb") as file:
    secrets = tomllib.load(file)


def update():
    stream = BCTWSConnection(
        UDID="OBC-AG-SA-C251065",
        username=secrets["username"],
        password=secrets["password"],
        beaconAddress="https://realtime.us.beacon.1.api.bluecity.ai/",
        singleton=True,
        subscriptions=[
            BCTWSConnection.subscriptionOption.FRAME,
            BCTWSConnection.subscriptionOption.PHASE_CHANGE,
            BCTWSConnection.subscriptionOption.PHASE_TIME_TO_CHANGE,
            # BCTWSConnection.subscriptionOption.LOOP_CHANGE,
        ],
    )

    latest_status = {}  # Dictionary to store the latest status for each phase

    while True:
        recv_time = datetime.now(timezone.utc)

        data = stream.get_hyperparameter()
        msg_time = datetime.fromisoformat(data.timestamp)

        if msg_time.tzinfo is None:
            msg_time = msg_time.replace(tzinfo=timezone.utc)

        latency = (recv_time - msg_time).total_seconds() * 1000  # ms

        frame = data.frame
        phase_change = data.phaseChange
        phase_TTC = data.phaseTimeToChange

        for phase in phase_change.phases:
            latest_status[phase.phaseNumber] = phase.status

        print(f"{'Phase':<8}{'Status':<10}{'Min TTC':<10}{'Max TTC':<10}")
        print("-" * 38)

        for phase in phase_TTC.phases[:16]:
            status = latest_status.get(phase.phaseNumber, "—")
            print(
                f"{phase.phaseNumber:<8}{status:<10}{phase.minTimeToChange:<10.1f}{phase.maxTimeToChange:<10.1f}"
            )

        print("-" * 38)
        print(f"Objects detected: {len(frame.objects)}")
        print(f"Latency (edge box to now): {latency:.2f} ms")

        # time.sleep(1)


if __name__ == "__main__":
    update()
