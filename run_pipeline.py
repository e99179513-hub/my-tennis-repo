# run_pipeline.py - wrapper that imports pipeline and saves outputs
from datetime import datetime
import os

try:
    from pipeline import fetch_sofascore_matches, run_pipeline
except Exception as e:
    raise RuntimeError("pipeline.py missing or import failed. Ensure pipeline.py exists in working directory.") from e

def main():
    matches = fetch_sofascore_matches()
    df_full, df_25, dashboard, errors = run_pipeline(matches)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    os.makedirs("outputs", exist_ok=True)
    df_full.to_csv(f"outputs/df_full_{ts}.csv", index=False)
    df_25.to_csv(f"outputs/df_25_{ts}.csv", index=False)
    dashboard.to_csv(f"outputs/dashboard_{ts}.csv", index=False)
    if errors:
        print("Pipeline completed with errors:")
        print(errors)
    else:
        print("Pipeline completed successfully. Files saved to outputs/")

if __name__ == "__main__":
    main()
