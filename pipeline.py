# pipeline.py - minimal, self-contained pipeline for Colab
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)

STYLE = {"T. Machac":"baseline","J. Munar":"clay specialist","A. Vukic":"neutral","T. Paul":"counterpunch",
         "B. Shelton":"serve-heavy","S. Báez":"baseline","M. McDonald":"baseline","L. Draxl":"neutral"}
SURFACE_PROFILE = {"Outdoor Hard":{"hold_bias":0.62,"break_bias":0.38,"tb_bias":0.48},
                   "Indoor Hard":{"hold_bias":0.65,"break_bias":0.35,"tb_bias":0.52}}

def fetch_sofascore_matches(date_str=None):
    # fallback slate for Colab runs
    return [
        ("T. Machac","J. Munar","ATP 250 Adelaide","Outdoor Hard"),
        ("A. Vukic","T. Paul","ATP 250 Adelaide","Outdoor Hard"),
        ("B. Shelton","S. Báez","ATP 250 Auckland","Outdoor Hard"),
        ("M. McDonald","L. Draxl","ATP AO Qualifying","Outdoor Hard")
    ]

def predict_match(a,b,tournament,surface):
    eloA, eloB = 1500, 1500
    gap = eloA - eloB
    tier = "Low"
    hist_conf = 70
    perc_conf = 72
    s = SURFACE_PROFILE.get(surface, {"hold_bias":0.6,"break_bias":0.4,"tb_bias":0.48})
    styleA, styleB = STYLE.get(a,"neutral"), STYLE.get(b,"neutral")
    corr = "Surface-driven Over"
    strength_score = round(abs(gap)/100,2)
    return {
        "Match": f"{a} vs {b}", "Tournament": tournament, "Surface": surface,
        "Confidence Tier": tier, "Historical Confidence": hist_conf, "Percentile Confidence": perc_conf,
        "UTR Rating Gap": round(gap/100,2), "Predicted Winner": a,
        "Rationale Note": f"{a} ({styleA}) vs {b} ({styleB}) -> {corr}",
        "Win a Set": "Possible", "Over 8.5 Games": 22.0, "Over Total Games": 22.0,
        "First Set TB": "No", "Correlation Note": corr,
        "Set Winner Strength Score": strength_score
    }

def run_pipeline(matches_list):
    rows, errors = [], []
    for m in matches_list:
        try:
            rows.append(predict_match(*m))
        except Exception as e:
            errors.append((m, str(e)))
    df_full = pd.DataFrame(rows)
    if not df_full.empty:
        df_full["Composite Score"] = 0.5
        df_25 = df_full.sort_values(by="Composite Score", ascending=False).head(25)
    else:
        df_25 = pd.DataFrame()
    dashboard = df_25.groupby("Tournament").agg({"Composite Score":"mean"}).reset_index() if not df_25.empty else pd.DataFrame()
    return df_full, df_25, dashboard, errors
