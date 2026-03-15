import sys
import os
import time
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.config.dask_config import start_dask
from backend.processing.pipeline import build_pipeline
from backend.anomaly.detector import detect_anomaly
from backend.config.email_config import send_anomaly_email

# Load environment variables
load_dotenv()
ADMIN_EMAIL = os.getenv("ADMIN_RECEIVER")


def main():
    print("Starting Log Analytics Engine...")

    # Start Dask Cluster
    client = start_dask()
    print(f"Dask Cluster Ready: {client.dashboard_link}")

    print("\n" + "=" * 50)
    print("Real-time Security Monitor Active...")
    print("=" * 50)

    # Path to realtime logs
    csv_path = "realtime_logs.csv"

    try:
        while True:

            # Check if log file exists
            if not os.path.exists(csv_path):
                print(f"Waiting for {csv_path}... (Ensure generator.py is running)", end="\r")
                time.sleep(2)
                continue

            # Build processing pipeline
            log_df = build_pipeline(csv_path)
            total_logs = len(log_df)

            # Detect anomalies
            anomalies = detect_anomaly(log_df, z_threshold=1)

            if not anomalies.empty:
                print(f"\nALERT: {len(anomalies)} anomalies detected!")

                for _, row in anomalies.iterrows():
                    anomaly_data = {
                        "timestamp": row["timestamp"],
                        "error_count": row["error_count"],
                        "z_score": row["z_score"]
                    }

                    # Send email alert
                    send_anomaly_email(
                        to_email=ADMIN_EMAIL,
                        anomaly=anomaly_data
                    )

                    print(
                        f"Email sent for spike at {row['timestamp']} "
                        f"(Z-Score: {round(row['z_score'], 2)})"
                    )

            # Status message
            print(
                f"Monitoring... Total logs: {total_logs}. Next scan in 10s.",
                end="\r"
            )

            time.sleep(10)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")

    except Exception as e:
        print(f"\nError: {e}")

    finally:
        print("Cleaning up Dask cluster...")
        client.close()


if __name__ == "__main__":
    main()
