#!/usr/bin/env python3
"""
================================================================================
Outage & Click Count Map Generator
================================================================================
PURPOSE:
    Reads Click_Count_and_Outage_Management_Data.xlsx and produces a single
    self-contained HTML file that renders an interactive map of Georgia showing:
      - Recloser dots coloured green → red by click count severity
      - Substation rings sized by number of outages
      - A toggleable click-count heatmap
      - A toggleable outage severity heatmap
      - Sidebar panels for browsing and filtering all data
      - A "Flagged" panel listing every row that was skipped due to missing data

USAGE:
    python3 generate_map.py                              # uses defaults below
    python3 generate_map.py my_data.xlsx                 # custom input file
    python3 generate_map.py my_data.xlsx my_output.html  # custom input + output

DEPENDENCIES:
    pip install pandas openpyxl
    (hashlib, json, sys, os, datetime are all part of the Python standard library)

STRUCTURE OF THIS FILE:
    1.  Imports & Config          – file paths, district coordinates
    2.  district_offset()         – spreads substation dots so they don't overlap
    3.  load_data()               – reads & validates all sheets, flags bad rows
    4.  aggregate_data()          – summarises data per-substation and per-recloser
    5.  click_color()             – converts a 0–1 score to a hex colour
    6.  build_html()              – assembles the full HTML/CSS/JS output string
    7.  main()                    – entry point: orchestrates steps 3 → 6

DEBUG TIPS:
    • If the script errors on load, check that EXCEL_PATH points to the right file
      and that the sheet names in the workbook still match the strings in load_data().
    • Add  print(df.head())  or  print(df.dtypes)  right after any xl.parse() call
      to inspect what pandas actually read in.
    • If dots don't appear on the map, open outage_map.html in a browser, press F12,
      go to the Console tab, and look for JavaScript errors.
    • If the heatmap layers never show data, check that RECLOSERS / SUBSTATIONS arrays
      in the browser console (F12 → Console → type RECLOSERS) have lat/lon values.
================================================================================
"""

# ── Standard library imports ──────────────────────────────────────────────────
import json        # Used to serialise Python dicts/lists into JavaScript arrays
import sys         # Used for sys.argv (command-line arguments) and sys.exit()
import os          # Used to check that the input file exists before opening it
import hashlib     # Used to generate deterministic (repeatable) fake coordinates
                   # from device IDs — same ID always gets the same map position
from datetime import datetime  # Used only in safe_str() to format timestamps

# ── Third-party imports ───────────────────────────────────────────────────────
import pandas as pd  # The main data-processing library.
                     # Install with:  pip install pandas openpyxl
                     # openpyxl is the engine pandas uses to read .xlsx files.

# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
#
# These are the only values you should normally need to change:
#   • EXCEL_PATH  – path to the workbook (relative to where you run the script)
#   • OUTPUT_HTML – name/path of the HTML file that will be created
#   • DISTRICT_COORDS – lat/lon centre points for each district
#
# If the source workbook is renamed or moved, update EXCEL_PATH.
# If new districts are added to the data, add them to DISTRICT_COORDS.
# ─────────────────────────────────────────────────────────────────────────────

# Default input file — override via command-line argument (see main() below)
EXCEL_PATH = "Click_Count_and_Outage_Management_Data.xlsx"

# Default output file — will be created (or overwritten) in the current directory
OUTPUT_HTML = "outage_map.html"

# Geographic centre of each district in NE Georgia (latitude, longitude).
# These are used as anchor points when spreading substation dots across the map.
# Source: approximate city-centre coordinates from Google Maps.
# DEBUG: If substations appear in the wrong area, adjust these values.
DISTRICT_COORDS = {
    "Gainesville":    (34.2979, -83.8241),
    "Jefferson":      (34.1123, -83.5999),
    "Lawrenceville":  (33.9566, -83.9880),
    "Neese":          (34.1500, -83.9000),  # approximate — no exact city centre
}


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2 — COORDINATE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def district_offset(name: str, district: str) -> tuple:
    """
    Generate a small but stable (deterministic) lat/lon offset for a substation
    so that multiple substations in the same district don't all stack on the
    exact same map pixel.

    HOW IT WORKS:
        We hash the substation name using MD5, then extract two small numbers
        from that hash to use as a latitude and longitude nudge (±0.1 degrees,
        which is roughly ±11 km).  Because MD5 always produces the same output
        for the same input, a given substation always ends up at the same spot
        on the map — re-running the script won't shuffle things around.

    PARAMETERS:
        name     – substation name string (e.g. "Lidell Road A")
        district – district name string (must be a key in DISTRICT_COORDS,
                   or the fallback centre (34.0, -83.8) will be used)

    RETURNS:
        (lat, lon) tuple as floats

    DEBUG: If two substations overlap on the map, their names are probably
           identical or very similar.  You can increase the divisor (currently
           1000.0) to spread them further apart, at the cost of some going
           outside the district boundaries.
    """
    # MD5 the name to get a deterministic integer we can slice bits from
    h = int(hashlib.md5(name.encode()).hexdigest(), 16)

    # Pull two independent offsets from different bit ranges of the hash.
    # (h % 200) gives 0–199; subtracting 100 centres it at 0; dividing by
    # 1000.0 scales to ±0.1 degrees.
    lat_off = ((h % 200) - 100) / 1000.0        # latitude offset  ≈ ±11 km
    lon_off = (((h >> 8) % 200) - 100) / 1000.0  # longitude offset ≈ ±9 km
    #            ^^^^^^ bit-shift to use a different part of the hash

    # Look up the district's centre, defaulting to a central NE Georgia point
    base = DISTRICT_COORDS.get(district, (34.0, -83.8))

    return (base[0] + lat_off, base[1] + lon_off)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DATA LOADING & VALIDATION
# ════════════════════════════════════════════════════════════════════════════════

def load_data(path: str):
    """
    Open the Excel workbook and load two kinds of data:

      A) The outage event log  (sheet: "Outage Data 09-17 to 09-25")
      B) Daily click-count logs (all sheets whose name contains "Click Count")

    For each dataset we:
      1. Read the sheet(s) into a pandas DataFrame
      2. Identify which columns are critical (must not be null)
      3. Flag any row that is missing a critical value
      4. Split into a "clean" DataFrame (used for analysis) and a
         "flagged" DataFrame (shown in the Flagged sidebar panel)

    PARAMETERS:
        path – path string to the .xlsx file

    RETURNS:
        outage          – clean outage rows (DataFrame)
        flagged_outages – outage rows with missing critical data (DataFrame)
        clicks          – clean click-count rows (DataFrame)
        flagged_clicks  – click-count rows with missing critical data (DataFrame)

    DEBUG:
        • "ERROR: File not found" → check that EXCEL_PATH is correct and that
          you're running the script from the right working directory.
        • "KeyError: 'Outage Data ...'" → the sheet was renamed in the workbook.
          Print xl.sheet_names to see what sheets actually exist.
        • Unexpected flagged count → add  print(outage_raw[outage_raw['_flagged']])
          just before the return to inspect which rows are being flagged and why.
    """

    # Verify the file exists before pandas tries to open it — gives a cleaner
    # error message than the cryptic FileNotFoundError pandas would raise.
    if not os.path.exists(path):
        print(f"ERROR: File not found: {path}", file=sys.stderr)
        print("  → Check that EXCEL_PATH is set correctly and you are running",
              file=sys.stderr)
        print("    the script from the directory that contains the .xlsx file.",
              file=sys.stderr)
        sys.exit(1)

    # Open the workbook. ExcelFile is used (rather than read_excel directly)
    # so we can inspect sheet names and parse multiple sheets efficiently.
    xl = pd.ExcelFile(path)
    print(f"✓ Loaded workbook with sheets: {xl.sheet_names}")

    # ── A) OUTAGE DATA ──────────────────────────────────────────────────────
    # Parse the single outage sheet into a DataFrame.
    # DEBUG: If this raises KeyError, the sheet was renamed — update the string below.
    outage_raw = xl.parse("Outage Data 09-17 to 09-25")

    # These four columns are the minimum required for a row to be useful.
    # A row missing any of these cannot be reliably displayed on the map.
    # "Outage"       – unique event ID (needed to count distinct outages)
    # "Time Off"     – when power went out (needed for time-series analysis)
    # "District"     – which district (needed to place the dot in the right area)
    # "Map Location" – device/location identifier (needed for geographic lookup)
    critical_cols = ["Outage", "Time Off", "District", "Map Location"]

    # Build a list of which critical columns are null for each row.
    # isnull() returns True/False per cell; we collect the column names where True.
    # axis=1 means "apply this function row-by-row".
    outage_raw["_missing_fields"] = outage_raw[critical_cols].isnull().apply(
        lambda row: [col for col, is_null in row.items() if is_null], axis=1
    )

    # A row is flagged if its _missing_fields list is non-empty (len > 0)
    outage_raw["_flagged"] = outage_raw["_missing_fields"].apply(lambda x: len(x) > 0)

    # Split into flagged and clean subsets.
    # ~ is the pandas "not" operator, so ~_flagged means "not flagged" = clean.
    flagged_outages = outage_raw[outage_raw["_flagged"]].copy()
    outage = outage_raw[~outage_raw["_flagged"]].copy()

    print(f"  Outage rows: {len(outage_raw)} total | "
          f"{len(outage)} valid | {len(flagged_outages)} flagged/skipped")

    # ── B) CLICK COUNT DATA ─────────────────────────────────────────────────
    # There are multiple daily click-count sheets (one per day of data).
    # We loop through all sheets and concatenate the ones matching "Click Count".
    all_clicks = []
    for sheet in xl.sheet_names:
        if "Click Count" in sheet:
            df = xl.parse(sheet)

            # Tag each row with its source sheet so the Flagged panel can show
            # which day a problematic record came from.
            df["_source_sheet"] = sheet

            all_clicks.append(df)
            print(f"    Loaded click sheet: {sheet} ({len(df)} rows)")

    # Stack all daily DataFrames into one.
    # ignore_index=True resets the row numbers so they run 0, 1, 2, ...
    # instead of repeating 0–N for each sheet.
    clicks_raw = pd.concat(all_clicks, ignore_index=True)

    # Critical columns for click-count rows:
    # "RepId"             – the recloser device ID (cannot map without this)
    # "MeterId"           – the meter linked to this recloser
    # "ChangeWindowStart" – the start of the time window (needed for time context)
    # "ClickCountChange"  – the actual click count we're aggregating
    click_critical = ["RepId", "MeterId", "ChangeWindowStart", "ClickCountChange"]

    clicks_raw["_missing_fields"] = clicks_raw[click_critical].isnull().apply(
        lambda row: [col for col, is_null in row.items() if is_null], axis=1
    )
    clicks_raw["_flagged"] = clicks_raw["_missing_fields"].apply(lambda x: len(x) > 0)

    flagged_clicks = clicks_raw[clicks_raw["_flagged"]].copy()
    clicks = clicks_raw[~clicks_raw["_flagged"]].copy()

    print(f"  Click count rows: {len(clicks_raw)} total | "
          f"{len(clicks)} valid | {len(flagged_clicks)} flagged/skipped")

    return outage, flagged_outages, clicks, flagged_clicks


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DATA AGGREGATION
# ════════════════════════════════════════════════════════════════════════════════

def aggregate_data(outage: pd.DataFrame, clicks: pd.DataFrame):
    """
    Condense the raw row-level data down to one summary record per substation
    (for outage data) and one summary record per recloser RepId (for click data).
    These summaries are what actually get displayed on the map.

    OUTAGE AGGREGATION (per substation):
        Groups rows by (Sub, District) and computes:
          - outage_count          : total number of outage events
          - total_customer_minutes: sum of customer-minutes lost
          - avg_duration_hrs      : mean outage duration in hours
          - customers_affected    : total customers impacted
          - causes                : top-3 cause descriptions by frequency

    CLICK AGGREGATION (per RepId):
        Groups rows by RepId and computes:
          - total_clicks          : sum of all ClickCountChange values
          - event_count           : number of time-window records
          - avg_clicks_per_window : mean clicks per window
          - max_window_clicks     : single highest window value (peak activity)
          - first_seen / last_seen: date range of activity

    COORDINATES:
        Neither dataset contains real GPS coordinates.
          • Substations are placed by hashing their name → offset from district centre
          • Reclosers are placed by hashing their RepId → random-looking but stable
            position within the NE Georgia bounding box

    PARAMETERS:
        outage – clean outage DataFrame from load_data()
        clicks – clean click-count DataFrame from load_data()

    RETURNS:
        sub_agg – per-substation summary DataFrame with lat/lon columns
        rep_agg – per-recloser summary DataFrame with lat/lon columns

    DEBUG:
        • If sub_agg is empty, the "Sub" or "District" columns may be named
          differently in the workbook. Check with:  print(outage.columns.tolist())
        • If click scores are all 0 or all 1, total_clicks may be constant.
          Check:  print(rep_agg["total_clicks"].describe())
        • If a recloser has real coordinates in the source data, replace the
          rep_coords() logic below with a join to that location table.
    """

    # ── A) PER-SUBSTATION OUTAGE SUMMARY ────────────────────────────────────

    # Group by both Sub (substation name) and District so we know which district
    # each substation belongs to (needed for coordinate generation).
    # dropna=True skips groups where Sub or District is null.
    sub_agg = (
        outage.groupby(["Sub", "District"], dropna=True)
        .agg(
            # Count rows = count distinct outage events for this substation
            outage_count=("Outage", "count"),

            # Sum of customer-minutes tells us total impact (customers × minutes)
            total_customer_minutes=("Customer Minutes", "sum"),

            # Duration is stored as a timedelta — convert to hours for readability.
            # hasattr guard handles any rows where Duration is NaN or a plain number.
            avg_duration_hrs=("Duration", lambda x: x.apply(
                lambda d: d.total_seconds() / 3600 if hasattr(d, "total_seconds") else 0
            ).mean()),

            # "# Out" = number of customers without power during the event
            customers_affected=("# Out", "sum"),

            # Return the top-3 most common cause descriptions as a {name: count} dict.
            # This dict is later rendered as a table in the substation popup.
            causes=("Cause Desc", lambda x: x.value_counts().head(3).to_dict()),
        )
        .reset_index()  # Move Sub and District back from index to regular columns
    )

    # Generate a lat/lon for each substation by hashing its name relative to
    # its district centre.  See district_offset() above for details.
    sub_agg[["lat", "lon"]] = sub_agg.apply(
        lambda r: pd.Series(district_offset(str(r["Sub"]), str(r["District"]))),
        axis=1,
    )

    # ── B) PER-RECLOSER CLICK SUMMARY ───────────────────────────────────────

    rep_agg = (
        clicks.groupby("RepId")
        .agg(
            # Sum all ClickCountChange values — this is our primary severity metric
            total_clicks=("ClickCountChange", "sum"),

            # How many time-window records exist for this device
            event_count=("ClickCountChange", "count"),

            # Average and maximum clicks per window give context on whether the
            # total is from many small events or a few large spikes
            avg_clicks_per_window=("ClickCountChange", "mean"),
            max_window_clicks=("ClickCountChange", "max"),

            # Date range of when this device was active in the dataset
            first_seen=("ChangeWindowStart", "min"),
            last_seen=("ChangeWindowStart", "max"),
        )
        .reset_index()
    )

    # Normalise total_clicks to a 0–1 "click_score" for colour mapping.
    # Formula: (value - min) / (max - min)
    # The max(..., 1) prevents divide-by-zero if all reclosers have identical counts.
    # score = 0.0  → minimum clicks in the dataset → GREEN dot
    # score = 1.0  → maximum clicks in the dataset → RED dot
    cmin = rep_agg["total_clicks"].min()
    cmax = rep_agg["total_clicks"].max()
    rep_agg["click_score"] = (rep_agg["total_clicks"] - cmin) / max(cmax - cmin, 1)

    # ── COORDINATE GENERATION FOR RECLOSERS ─────────────────────────────────
    # The click-count data has no geographic coordinates.
    # We pseudo-randomly spread each RepId within the NE Georgia bounding box
    # using the MD5 hash of the RepId so the positions are stable across runs.
    # DEBUG: If you later get real GPS data for reclosers (e.g. from a GIS export),
    #        replace this entire block with a merge/join on RepId.
    lat_min, lat_max = 33.7, 34.6   # Southern and northern edges of NE Georgia
    lon_min, lon_max = -84.3, -83.3  # Western and eastern edges of NE Georgia

    rep_agg = rep_agg.reset_index(drop=True)

    def rep_coords(rep_id):
        """Hash a RepId to a stable (lat, lon) within the NE Georgia bounding box."""
        h = int(hashlib.md5(str(rep_id).encode()).hexdigest(), 16)

        # Use different parts of the hash for lat and lon so they're independent.
        # (h % 10000) / 10000 gives a 0–1 fraction; multiply by the degree range
        # and add the minimum to land within the bounding box.
        lat = lat_min + (h % 10000) / 10000 * (lat_max - lat_min)
        lon = lon_min + ((h >> 16) % 10000) / 10000 * (lon_max - lon_min)
        #                  ^^^^^^ bit-shift 16 positions to use a different part
        return lat, lon

    # Apply rep_coords to every row and assign the two results to lat/lon columns.
    # pd.Series with index=["lat","lon"] lets pandas unpack the tuple properly.
    coords = rep_agg["RepId"].apply(
        lambda rid: pd.Series(rep_coords(rid), index=["lat", "lon"])
    )
    rep_agg["lat"] = coords["lat"]
    rep_agg["lon"] = coords["lon"]

    print(f"  Substations summarised: {len(sub_agg)}")
    print(f"  Reclosers summarised:   {len(rep_agg)}")

    return sub_agg, rep_agg


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 5 — COLOUR UTILITY
# ════════════════════════════════════════════════════════════════════════════════

def click_color(score: float) -> str:
    """
    Convert a normalised click score (0.0 to 1.0) into a hex colour string
    that transitions from bright green → yellow → red.

    COLOUR LOGIC:
        score = 0.0  →  #00ff00  (pure green)
        score = 0.5  →  #ffff00  (yellow — both R and G are 255)
        score = 1.0  →  #ff0000  (pure red)

    The trick is that R and G mirror each other on opposite halves of the range:
        R = min(255, score × 2 × 255)   — climbs from 0 to 255 in the first half
        G = min(255, (1−score) × 2 × 255) — falls from 255 to 0 in the second half

    PARAMETERS:
        score – float between 0.0 and 1.0

    RETURNS:
        hex colour string, e.g. "#ff8000"

    DEBUG: Call click_color(0), click_color(0.5), click_color(1) to verify the
           three anchor colours are correct before running the full script.
    """
    r = int(min(255, score * 2 * 255))        # Red channel
    g = int(min(255, (1 - score) * 2 * 255))  # Green channel
    b = 0                                      # Blue is always 0 in this palette
    return f"#{r:02x}{g:02x}{b:02x}"
    #           ^^  :02x formats as 2-digit hex with leading zero if needed


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 6 — HTML GENERATION
# ════════════════════════════════════════════════════════════════════════════════

def build_html(sub_agg: pd.DataFrame, rep_agg: pd.DataFrame,
               flagged_outages: pd.DataFrame, flagged_clicks: pd.DataFrame) -> str:
    """
    Assemble and return a complete, self-contained HTML string that can be saved
    to a .html file and opened directly in any modern browser (no server needed).

    ARCHITECTURE OF THE OUTPUT FILE:
        The HTML file is structured as a Python f-string template.
        Python data is injected as JSON constants at the top of the <script> block,
        then all rendering logic is pure JavaScript (Leaflet.js for mapping).

        ┌─────────────────────────────────────┐
        │  <head>                              │
        │    CSS styles (dark theme)           │
        │    Leaflet CSS (CDN)                 │
        │  </head>                             │
        │  <body>                              │
        │    Header bar (summary stats)        │
        │    Sidebar (tabs: Controls/Reclosers │
        │             /Outages/Flagged)        │
        │    #map div (full-height Leaflet map)│
        │    <script>                          │
        │      const RECLOSERS   = [...];  ← Python JSON injected here
        │      const SUBSTATIONS = [...];
        │      const FLAGGED_OUT = [...];
        │      const FLAGGED_CLK = [...];
        │      // All map/UI logic follows...  │
        │    </script>                         │
        │  </body>                             │
        └─────────────────────────────────────┘

    EXTERNAL LIBRARIES (loaded from CDN — requires internet on first open):
        • Leaflet 1.9.4   – the core mapping library (tiles, markers, popups)
        • leaflet.heat    – heatmap layer plugin for Leaflet
        • Google Fonts    – Share Tech Mono + Barlow

    PARAMETERS:
        sub_agg         – per-substation summary DataFrame (from aggregate_data)
        rep_agg         – per-recloser summary DataFrame (from aggregate_data)
        flagged_outages – rows skipped due to missing outage data
        flagged_clicks  – rows skipped due to missing click data

    RETURNS:
        A single string containing the entire HTML document.

    DEBUG:
        • To inspect the raw JSON data injected into the HTML, open the output
          file in a text editor and search for "const RECLOSERS".
        • If the map is blank (no dots), check that the lat/lon values in
          RECLOSERS/SUBSTATIONS are within Georgia's bounding box (~33–35°N,
          ~81–85°W). Values outside this range won't appear at the default zoom.
        • If popups don't open, check the browser console (F12) for JS errors.
          The most common cause is a RepId or substation name containing a
          special character that breaks the HTML id attribute — the replace()
          calls in buildSubstationPopup() handle spaces, but add more as needed.
    """

    # ── HELPER: safe_str ────────────────────────────────────────────────────
    def safe_str(v):
        """
        Convert any Python value to a plain string safe for JSON serialisation.
        Handles three special cases that would otherwise break json.dumps():
          • NaN / None / NaT  → returns the string "N/A"
          • pandas Timestamp  → formats as "YYYY-MM-DD HH:MM"
          • everything else   → str() conversion
        """
        if pd.isna(v):
            return "N/A"
        if isinstance(v, (pd.Timestamp, datetime)):
            return v.strftime("%Y-%m-%d %H:%M")
        return str(v)

    # ── STEP 1: Build the recloser data list ────────────────────────────────
    # Each dict here becomes one element of the JavaScript RECLOSERS array.
    # Every key maps directly to a property accessed in the JS popup templates.
    # If you add a new field here, add a corresponding display row in
    # buildRecloserPopup() in the JS section below.
    reclosers_js = []
    for _, r in rep_agg.iterrows():
        color = click_color(float(r["click_score"]))  # Hex colour for this dot
        reclosers_js.append({
            "repId":      int(r["RepId"]),            # Recloser device identifier
            "lat":        float(r["lat"]),             # Map latitude
            "lon":        float(r["lon"]),             # Map longitude
            "totalClicks":    int(r["total_clicks"]),  # Sum of all click counts
            "eventCount":     int(r["event_count"]),   # Number of time-window records
            "avgClicks":  round(float(r["avg_clicks_per_window"]), 1),
            "maxWindow":      int(r["max_window_clicks"]),  # Peak single-window count
            "firstSeen":  safe_str(r["first_seen"]),   # Earliest record date
            "lastSeen":   safe_str(r["last_seen"]),    # Latest record date
            "score":      float(r["click_score"]),     # 0–1 normalised score
            "color":      color,                       # Pre-computed hex colour
        })

    # ── STEP 2: Build the substation data list ──────────────────────────────
    # Each dict becomes one element of the JavaScript SUBSTATIONS array.
    substations_js = []
    for _, r in sub_agg.iterrows():
        substations_js.append({
            "sub":              safe_str(r["Sub"]),
            "district":         safe_str(r["District"]),
            "lat":              float(r["lat"]),
            "lon":              float(r["lon"]),
            "outageCount":      int(r["outage_count"]),
            "customerMinutes":  int(r["total_customer_minutes"]),
            "avgDuration":      round(float(r["avg_duration_hrs"]), 2),
            "customersAffected":int(r["customers_affected"]),
            "causes":           r["causes"],   # dict of {cause_description: count}
        })

    # ── STEP 3: Build the flagged-entry lists ───────────────────────────────
    # These are displayed in the "Flagged" sidebar tab so operators can
    # identify and fix data quality issues at the source.
    flagged_out_js = []
    for _, r in flagged_outages.iterrows():
        flagged_out_js.append({
            "outage":  safe_str(r.get("Outage")),       # Outage event ID
            "missing": r["_missing_fields"],             # List of missing column names
            "timeOff": safe_str(r.get("Time Off")),     # When the outage started
            "district":safe_str(r.get("District")),
        })

    flagged_click_js = []
    for _, r in flagged_clicks.iterrows():
        flagged_click_js.append({
            "repId":   safe_str(r.get("RepId")),
            "missing": r["_missing_fields"],
            "sheet":   safe_str(r.get("_source_sheet")),  # Which daily sheet it came from
        })

    # ── STEP 4: Serialise all four lists to JSON strings ────────────────────
    # json.dumps() converts Python lists/dicts to valid JavaScript literal syntax.
    # These strings are embedded verbatim inside the <script> block below as
    # JavaScript const declarations.
    reclosers_json     = json.dumps(reclosers_js)
    substations_json   = json.dumps(substations_js)
    flagged_out_json   = json.dumps(flagged_out_js)
    flagged_click_json = json.dumps(flagged_click_js)

    # ── STEP 5: Compute summary totals for the header bar ───────────────────
    total_clicks   = int(rep_agg["total_clicks"].sum())
    total_outages  = int(sub_agg["outage_count"].sum())
    total_cust_min = int(sub_agg["total_customer_minutes"].sum())
    flag_count     = len(flagged_outages) + len(flagged_clicks)

    # ── STEP 6: Build and return the HTML f-string ──────────────────────────
    # Important f-string escaping note:
    #   Because this is a Python f-string, any literal curly braces in the HTML
    #   or JavaScript must be doubled: {{ }} instead of { }.
    #   Only {python_variable} (single braces) are interpolated by Python.
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Georgia Outage &amp; Recloser Dashboard</title>

<!-- Google Fonts: Share Tech Mono (monospace for labels) + Barlow (body text) -->
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&display=swap" rel="stylesheet"/>

<!-- Leaflet CSS: styles for the map tiles, popups, zoom controls, etc. -->
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>

<!-- Leaflet JS: the core mapping library -->
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

<!-- Leaflet.heat plugin: adds the L.heatLayer() class used for heatmaps -->
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>

<style>
/* ── CSS CUSTOM PROPERTIES (variables) ──────────────────────────────────────
   Centralising colours here means you can re-theme the whole dashboard by
   changing these values.  DEBUG: If colours look wrong, check here first. */
:root {{
  --bg: #0b0f14;       /* Page background (near-black dark blue) */
  --panel: #111620;    /* Sidebar and header background */
  --border: #1e2d42;   /* Subtle borders between UI elements */
  --accent: #00c8ff;   /* Primary interactive colour (cyan) */
  --accent2: #ff6b35;  /* Warning/flagged colour (orange) */
  --text: #d4e4f0;     /* Body text colour */
  --muted: #5a7a96;    /* Secondary/label text colour */
  --green: #00e676;    /* "Low" severity badge colour */
  --red: #ff1744;      /* "High" severity badge colour */
  --yellow: #ffd600;   /* "Medium" severity badge colour */
  --radius: 8px;       /* Border-radius for cards and panels */
}}

/* Global reset — removes browser default margin/padding */
* {{ box-sizing: border-box; margin: 0; padding: 0; }}

/* Full-viewport layout: header + (sidebar | map) stacked vertically */
body {{
  font-family: 'Barlow', sans-serif;
  background: var(--bg);
  color: var(--text);
  display: flex;
  flex-direction: column;
  height: 100vh;     /* Fill the full browser window height */
  overflow: hidden;  /* Prevent outer scroll — inner elements scroll individually */
}}

/* ── HEADER BAR ─────────────────────────────────────────────────────────────
   Fixed-height strip at the top showing title + summary stat chips. */
header {{
  background: var(--panel);
  border-bottom: 1px solid var(--border);
  padding: 0 20px;
  height: 56px;
  display: flex;
  align-items: center;
  gap: 24px;
  flex-shrink: 0;  /* Don't let flexbox shrink the header */
  z-index: 1000;   /* Sit above the Leaflet map tiles */
}}

header h1 {{
  font-family: 'Share Tech Mono', monospace;
  font-size: 15px;
  color: var(--accent);
  letter-spacing: 2px;
  text-transform: uppercase;
  white-space: nowrap;  /* Prevent the title from wrapping on narrow screens */
}}

/* Container for the stat chips in the top-right of the header */
.header-stats {{
  display: flex;
  gap: 20px;
  margin-left: auto;  /* Push to the right edge */
}}

/* Each individual stat "LABEL value" chip */
.stat-chip {{
  font-size: 11px;
  color: var(--muted);
  font-family: 'Share Tech Mono', monospace;
  letter-spacing: 1px;
}}

/* The numeric value inside each chip (highlighted) */
.stat-chip span {{
  color: var(--accent);
  font-weight: 700;
}}

/* ── MAIN LAYOUT ─────────────────────────────────────────────────────────────
   Row containing sidebar (fixed width) + map (fills remaining space). */
.layout {{
  display: flex;
  flex: 1;           /* Take all remaining height below the header */
  overflow: hidden;  /* Clip any overflow — children handle their own scroll */
}}

/* ── SIDEBAR ─────────────────────────────────────────────────────────────────
   Fixed-width left panel containing tabs and control UI. */
#sidebar {{
  width: 300px;
  background: var(--panel);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  flex-shrink: 0;  /* Don't let the map squash the sidebar */
  overflow: hidden;
}}

/* Tab button row at the top of the sidebar */
.sidebar-tabs {{
  display: flex;
  border-bottom: 1px solid var(--border);
}}

/* Individual tab buttons (Controls / Reclosers / Outages / Flagged) */
.tab-btn {{
  flex: 1;           /* All tabs share equal width */
  padding: 10px 6px;
  background: none;
  border: none;
  border-bottom: 2px solid transparent;  /* Active state adds a colour here */
  color: var(--muted);
  font-family: 'Share Tech Mono', monospace;
  font-size: 10px;
  letter-spacing: 1px;
  cursor: pointer;
  transition: all .2s;
  text-transform: uppercase;
}}

.tab-btn.active {{
  color: var(--accent);
  border-bottom-color: var(--accent);  /* Cyan underline on active tab */
}}

.tab-btn:hover:not(.active) {{
  color: var(--text);
}}

/* Each tab's content pane — hidden by default, shown when .active */
.tab-pane {{
  display: none;
  flex: 1;
  overflow-y: auto;  /* Independently scrollable content area */
  padding: 12px;
}}

.tab-pane.active {{
  display: block;
}}

/* ── CONTROL SECTION ─────────────────────────────────────────────────────────
   Grouped blocks within the Controls tab. */
.control-section {{
  margin-bottom: 16px;
}}

/* Section heading label (e.g. "LAYERS", "FILTER: MIN CLICK COUNT") */
.control-label {{
  font-size: 10px;
  font-family: 'Share Tech Mono', monospace;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 1.5px;
  margin-bottom: 8px;
}}

/* Row containing a text label on the left and a toggle switch on the right */
.toggle-row {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 0;
  border-bottom: 1px solid var(--border);
}}

.toggle-row label {{
  font-size: 13px;
  color: var(--text);
  cursor: pointer;
}}

/* ── CUSTOM TOGGLE SWITCH ───────────────────────────────────────────────────
   CSS-only iOS-style toggle built on top of a hidden <input type="checkbox">.
   The visible track is .toggle-slider; the thumb is its ::before pseudo-element.
   Checking the checkbox triggers the :checked CSS state which moves the thumb
   and changes the track colour via the adjacent-sibling combinator (+). */
.toggle {{
  position: relative;
  width: 38px;
  height: 20px;
}}

.toggle input {{
  opacity: 0;
  width: 0;
  height: 0;  /* Invisible but still clickable via the parent <label> */
}}

.toggle-slider {{
  position: absolute;
  inset: 0;
  background: var(--border);
  border-radius: 20px;
  cursor: pointer;
  transition: .2s;
}}

/* The circular thumb */
.toggle-slider::before {{
  content: '';
  position: absolute;
  width: 14px;
  height: 14px;
  left: 3px;
  top: 3px;
  background: var(--muted);
  border-radius: 50%;
  transition: .2s;
}}

/* When checked: tint the track cyan */
.toggle input:checked + .toggle-slider {{
  background: rgba(0,200,255,.25);
}}

/* When checked: slide the thumb right and colour it cyan */
.toggle input:checked + .toggle-slider::before {{
  transform: translateX(18px);
  background: var(--accent);
}}

/* ── RANGE SLIDERS ───────────────────────────────────────────────────────────
   Used for Min Click Count and Dot Size controls. */
.slider-row {{
  padding: 8px 0;
  border-bottom: 1px solid var(--border);
}}

.slider-row input[type=range] {{
  width: 100%;
  accent-color: var(--accent);  /* CSS accent-color tints the slider thumb */
  margin-top: 6px;
}}

/* The numeric readout that updates as the slider moves */
.slider-val {{
  font-family: 'Share Tech Mono', monospace;
  font-size: 11px;
  color: var(--accent);
  float: right;
}}

/* ── COLOUR LEGEND ───────────────────────────────────────────────────────────
   A gradient bar below the Controls tab showing the green→red colour scale. */
.legend-bar {{
  height: 10px;
  border-radius: 5px;
  background: linear-gradient(to right, #00ff00, #ffff00, #ff0000);
  margin: 6px 0;
}}

.legend-labels {{
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: var(--muted);
  font-family: 'Share Tech Mono', monospace;
}}

/* ── SIDEBAR LIST ITEMS ──────────────────────────────────────────────────────
   Cards in the Reclosers and Outages tabs.
   Clicking any card calls flyTo() to pan the map to that feature. */
.info-item {{
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 10px;
  margin-bottom: 8px;
  cursor: pointer;
  transition: border-color .2s;
}}

.info-item:hover {{
  border-color: var(--accent);  /* Highlight border on hover */
}}

.info-item .iname {{
  font-size: 13px;
  font-weight: 600;
  color: var(--accent);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;  /* "Rep 123456..." if name is too long */
}}

.info-item .imeta {{
  font-size: 11px;
  color: var(--muted);
  margin-top: 3px;
  font-family: 'Share Tech Mono', monospace;
}}

/* ── SEVERITY BADGES ─────────────────────────────────────────────────────────
   Coloured pill labels shown in the Reclosers list and popups:
     badge-green  = LOW  (score < 0.35)
     badge-yellow = MED  (0.35 ≤ score < 0.70)
     badge-red    = HIGH (score ≥ 0.70) */
.badge {{
  display: inline-block;
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 10px;
  font-family: 'Share Tech Mono', monospace;
  font-weight: 700;
}}

.badge-red    {{ background: rgba(255,23,68,.2);   color: var(--red);    }}
.badge-yellow {{ background: rgba(255,214,0,.15);  color: var(--yellow); }}
.badge-green  {{ background: rgba(0,230,118,.15);  color: var(--green);  }}
.badge-blue   {{ background: rgba(0,200,255,.15);  color: var(--accent); }}

/* ── FLAGGED ITEMS ────────────────────────────────────────────────────────────
   Cards shown in the Flagged tab for entries that were excluded from analysis. */
.flagged-item {{
  background: rgba(255,107,53,.05);
  border: 1px solid rgba(255,107,53,.3);
  border-radius: var(--radius);
  padding: 8px 10px;
  margin-bottom: 6px;
  font-size: 11px;
}}

.flagged-item .ftitle {{
  color: var(--accent2);  /* Orange — stands out as a warning */
  font-family: 'Share Tech Mono', monospace;
  font-size: 11px;
  margin-bottom: 3px;
}}

.flagged-item .fmissing {{
  color: var(--muted);
}}

/* ── SCROLLBAR STYLING ───────────────────────────────────────────────────────
   Thin scrollbar for sidebar panes (WebKit only — Firefox uses scrollbar-width). */
::-webkit-scrollbar {{ width: 4px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 2px; }}

/* Match Leaflet's default map background to the dark theme */
.leaflet-container {{ background: #080f18 !important; }}

/* Tooltip shown when hovering over a Georgia county outline */
.county-tip {{
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 4px;
  color: var(--muted);
  font-family: 'Share Tech Mono', monospace;
  font-size: 10px;
  letter-spacing: 1px;
  padding: 3px 7px;
  box-shadow: none;
}}

/* ── LEAFLET POPUP OVERRIDES ─────────────────────────────────────────────────
   Override Leaflet's default white popup with our dark theme. */
.leaflet-popup-content-wrapper {{
  background: var(--panel);
  border: 1px solid var(--accent);
  border-radius: var(--radius);
  box-shadow: 0 0 20px rgba(0,200,255,.2);
  color: var(--text);
}}

.leaflet-popup-tip {{
  background: var(--accent);
}}

.leaflet-popup-content {{
  font-family: 'Barlow', sans-serif;
  margin: 10px 14px;
  min-width: 220px;
}}

/* Popup sections */
.popup-title {{
  font-family: 'Share Tech Mono', monospace;
  font-size: 13px;
  color: var(--accent);
  letter-spacing: 1px;
  margin-bottom: 10px;
  border-bottom: 1px solid var(--border);
  padding-bottom: 6px;
}}

/* Each data row: label on the left, value on the right */
.popup-row {{
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  padding: 3px 0;
  border-bottom: 1px solid var(--border);
}}

.popup-row:last-child {{ border-bottom: none; }}

.popup-key {{ color: var(--muted); }}
.popup-val {{ color: var(--text); font-weight: 600; }}

/* Advanced details section — hidden until the "Show Advanced" button is clicked */
.popup-advanced {{
  display: none;
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px solid var(--border);
}}

.popup-advanced.show {{ display: block; }}

/* The "Show Advanced / Hide Advanced" toggle button inside popups */
.detail-toggle {{
  width: 100%;
  margin-top: 8px;
  padding: 5px;
  background: rgba(0,200,255,.1);
  border: 1px solid var(--accent);
  border-radius: 4px;
  color: var(--accent);
  font-family: 'Share Tech Mono', monospace;
  font-size: 10px;
  letter-spacing: 1px;
  cursor: pointer;
  text-transform: uppercase;
  transition: background .2s;
}}

.detail-toggle:hover {{ background: rgba(0,200,255,.2); }}

/* Unused pulse animation — available for future high-priority marker highlighting */
@keyframes pulse {{
  0%   {{ opacity: 1; transform: scale(1);   }}
  50%  {{ opacity: .6; transform: scale(1.4);}}
  100% {{ opacity: 1; transform: scale(1);   }}
}}

.pulse-marker {{
  animation: pulse 1.8s ease-in-out infinite;
}}
</style>
</head>
<body>

<!-- ═══════════════════════════════════════════════════════════════════════════
     HEADER BAR
     Shows the dashboard title and live-computed summary stats from Python.
     The values in <span> tags are injected by the Python f-string.
     ════════════════════════════════════════════════════════════════════════ -->
<header>
  <h1>⚡ Georgia Grid Monitor</h1>
  <div class="header-stats">
    <div class="stat-chip">RECLOSERS <span>{len(rep_agg)}</span></div>
    <div class="stat-chip">OUTAGES <span>{total_outages}</span></div>
    <div class="stat-chip">CUST·MIN <span>{total_cust_min:,}</span></div>
    <div class="stat-chip">TOTAL CLICKS <span>{total_clicks:,}</span></div>
    <div class="stat-chip">FLAGGED <span style="color:var(--accent2)">{flag_count}</span></div>
  </div>
</header>

<!-- ═══════════════════════════════════════════════════════════════════════════
     MAIN LAYOUT: sidebar (left) + map (right, fills remaining width)
     ════════════════════════════════════════════════════════════════════════ -->
<div class="layout">

  <!-- ── SIDEBAR ────────────────────────────────────────────────────────── -->
  <div id="sidebar">

    <!-- Tab navigation row.  Each button calls switchTab() with its tab name. -->
    <div class="sidebar-tabs">
      <button class="tab-btn active" onclick="switchTab('controls')">Controls</button>
      <button class="tab-btn" onclick="switchTab('reclosers')">Reclosers</button>
      <button class="tab-btn" onclick="switchTab('outages')">Outages</button>
      <button class="tab-btn" onclick="switchTab('flagged')">Flagged</button>
    </div>

    <!-- ── CONTROLS TAB ─────────────────────────────────────────────────── -->
    <div id="tab-controls" class="tab-pane active">

      <!-- Layer toggles: each checkbox calls toggleLayer(name, checked) in JS -->
      <div class="control-section">
        <div class="control-label">Layers</div>
        <div class="toggle-row">
          <label for="tog-reclosers">Recloser Dots</label>
          <label class="toggle">
            <input type="checkbox" id="tog-reclosers" checked onchange="toggleLayer('reclosers', this.checked)"/>
            <span class="toggle-slider"></span>
          </label>
        </div>
        <div class="toggle-row">
          <label for="tog-substations">Substations</label>
          <label class="toggle">
            <input type="checkbox" id="tog-substations" checked onchange="toggleLayer('substations', this.checked)"/>
            <span class="toggle-slider"></span>
          </label>
        </div>
        <div class="toggle-row">
          <label for="tog-heatmap">Click Heatmap</label>
          <label class="toggle">
            <!-- Off by default — enabling rebuilds the heatmap layer -->
            <input type="checkbox" id="tog-heatmap" onchange="toggleLayer('heatmap', this.checked)"/>
            <span class="toggle-slider"></span>
          </label>
        </div>
        <div class="toggle-row">
          <label for="tog-outageheat">Outage Heatmap</label>
          <label class="toggle">
            <input type="checkbox" id="tog-outageheat" onchange="toggleLayer('outageheat', this.checked)"/>
            <span class="toggle-slider"></span>
          </label>
        </div>
      </div>

      <!-- Min Click Count slider: filters out reclosers below a threshold.
           max="400" should cover the data range — adjust if max clicks exceed 400. -->
      <div class="control-section">
        <div class="control-label">Filter: Min Click Count</div>
        <div class="slider-row">
          <span class="slider-val" id="min-click-val">0</span>
          <input type="range" id="min-click-slider" min="0" max="400" value="0"
                 oninput="filterMinClicks(+this.value)"/>
        </div>
      </div>

      <!-- Dot size slider: adjusts the radius of all recloser circle markers -->
      <div class="control-section">
        <div class="control-label">Dot Size</div>
        <div class="slider-row">
          <span class="slider-val" id="dot-size-val">7</span>
          <input type="range" id="dot-size-slider" min="3" max="20" value="7"
                 oninput="updateDotSize(+this.value)"/>
        </div>
      </div>

      <!-- Visual legend for the green→red colour scale -->
      <div class="control-section">
        <div class="control-label">Click Score Legend</div>
        <div class="legend-bar"></div>
        <div class="legend-labels">
          <span>Low</span><span>Medium</span><span>High</span>
        </div>
        <div style="font-size:11px;color:var(--muted);margin-top:8px;line-height:1.5">
          Dots represent individual reclosers.<br/>
          Colour = total click count over the reporting period.
        </div>
      </div>

      <!-- District filter: limits substation rings to one district -->
      <div class="control-section">
        <div class="control-label">District Filter</div>
        <select id="district-filter" onchange="filterDistrict(this.value)"
          style="width:100%;padding:6px;background:var(--bg);border:1px solid var(--border);
                 border-radius:4px;color:var(--text);font-family:inherit;font-size:12px">
          <option value="all">All Districts</option>
          <option value="Gainesville">Gainesville</option>
          <option value="Jefferson">Jefferson</option>
          <option value="Lawrenceville">Lawrenceville</option>
          <option value="Neese">Neese</option>
        </select>
      </div>

    </div><!-- /tab-controls -->

    <!-- ── RECLOSERS TAB ─────────────────────────────────────────────────── -->
    <!-- Populated dynamically by buildRecloserList() in JavaScript -->
    <div id="tab-reclosers" class="tab-pane">
      <div id="recloser-list"></div>
    </div>

    <!-- ── OUTAGES TAB ───────────────────────────────────────────────────── -->
    <!-- Populated dynamically by buildOutageList() in JavaScript -->
    <div id="tab-outages" class="tab-pane">
      <div id="outage-list"></div>
    </div>

    <!-- ── FLAGGED TAB ───────────────────────────────────────────────────── -->
    <!-- Populated dynamically by buildFlaggedList() in JavaScript -->
    <div id="tab-flagged" class="tab-pane">
      <div id="flagged-count" style="font-size:12px;color:var(--accent2);
           font-family:'Share Tech Mono',monospace;margin-bottom:10px;"></div>
      <div id="flagged-list"></div>
    </div>

  </div><!-- /sidebar -->

  <!-- ── MAP CONTAINER ──────────────────────────────────────────────────── -->
  <!-- Leaflet will initialise inside this div via L.map('map') in the script -->
  <div id="map"></div>

</div><!-- /layout -->

<script>
// ════════════════════════════════════════════════════════════════════════════
// JAVASCRIPT DATA — injected by Python at build time
// ════════════════════════════════════════════════════════════════════════════
// These four constants contain all the processed data from the Excel workbook.
// They are plain JavaScript arrays of objects — you can inspect them in the
// browser console (F12 → Console) by typing:  console.log(RECLOSERS[0])

const RECLOSERS   = {reclosers_json};   // One object per recloser RepId
const SUBSTATIONS = {substations_json}; // One object per (Sub, District) pair
const FLAGGED_OUT = {flagged_out_json}; // Outage rows that were missing critical data
const FLAGGED_CLK = {flagged_click_json}; // Click-count rows that were missing data

// ════════════════════════════════════════════════════════════════════════════
// MAP INITIALISATION
// ════════════════════════════════════════════════════════════════════════════
// Create a Leaflet map inside the #map div.
// center: lat/lon of NE Georgia centre; zoom: 9 shows the full service territory.
// DEBUG: If the map appears blank (no tiles), check your internet connection —
//        the dark tile layer is loaded from cartodb's CDN.
const map = L.map('map', {{
  center: [33.95, -83.85],  // NE Georgia centre point
  zoom: 9,                   // Zoom 9 ≈ county level
  zoomControl: true,
}});

// ── TILE LAYER WITH OFFLINE FALLBACK ─────────────────────────────────────────
// Try loading the CartoDB dark tile layer. If tiles fail (no internet), the map
// background stays dark and the Georgia GeoJSON outline below provides context.
const tileLayer = L.tileLayer(
  'https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    attribution: '© OpenStreetMap contributors © CARTO',
    maxZoom: 19,
    subdomains: 'abcd',
    errorTileUrl: 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7'
  }}
);
tileLayer.addTo(map);

// ── EMBEDDED GEORGIA COUNTY OUTLINES (offline fallback) ───────────────────────
// Simplified GeoJSON of Georgia's 159 counties — drawn as a lightweight SVG
// overlay so the map is useful even when tiles fail to load.
// Counties are styled with subtle borders; the NE Georgia service area counties
// (Hall, Banks, Jackson, Gwinnett, etc.) are highlighted slightly brighter.
// Source: US Census Bureau TIGER simplified boundaries (public domain).
const GA_SERVICE_COUNTIES = [
  "Hall","Banks","Jackson","Barrow","Gwinnett","Forsyth","Lumpkin",
  "White","Habersham","Stephens","Franklin","Madison","Oglethorpe"
];

fetch('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json')
  .then(r => r.json())
  .then(geojson => {{
    const gaCounties = {{
      type: 'FeatureCollection',
      features: geojson.features.filter(f => f.properties.STATE === '13')
    }};
    L.geoJSON(gaCounties, {{
      style: f => {{
        const name = (f.properties.NAME || '');
        const inService = GA_SERVICE_COUNTIES.includes(name);
        return {{
          color:       inService ? '#1e4060' : '#142030',
          weight:      inService ? 1.2 : 0.6,
          fillColor:   inService ? '#0d2035' : '#080f18',
          fillOpacity: 0.9,
        }};
      }},
      onEachFeature: (f, layer) => {{
        layer.bindTooltip(f.properties.NAME + ' County', {{
          permanent: false, direction: 'center',
          className: 'county-tip'
        }});
      }}
    }}).addTo(map);
  }})
  .catch(() => {{
    // If the GeoJSON CDN also fails, just draw a simple Georgia bounding box
    // so at least the service area is visible.
    L.rectangle([[30.36, -85.61], [35.00, -80.84]], {{
      color: '#1e4060', weight: 1.5,
      fillColor: '#080f18', fillOpacity: 0.9
    }}).addTo(map);
  }});

// ════════════════════════════════════════════════════════════════════════════
// LAYER GROUPS
// ════════════════════════════════════════════════════════════════════════════
// Leaflet LayerGroups bundle multiple markers together so we can show/hide
// an entire category of markers with a single map.addLayer() / removeLayer() call.
// heatmap and outageheat start as null — they're built lazily when first toggled.
const layers = {{
  reclosers:  L.layerGroup().addTo(map),  // All recloser circle markers
  substations:L.layerGroup().addTo(map),  // All substation circle markers
  heatmap:    null,   // Click-count heatmap (built by buildClickHeatmap)
  outageheat: null,   // Outage severity heatmap (built by buildOutageHeatmap)
}};

// ── Global state variables (shared between filter/slider callbacks) ────────
let dotRadius    = 7;    // Current recloser dot radius (pixels), set by slider
let minClicks    = 0;    // Current minimum click threshold, set by slider
let districtFilter = 'all'; // Current district filter value, set by dropdown

// ════════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════

/**
 * hexToRgb(hex)
 * Parse a "#rrggbb" hex colour string into [r, g, b] integer components.
 * Used when we need to create a semi-transparent border version of a fill colour.
 *
 * @param {{string}} hex - e.g. "#ff8000"
 * @returns {{number[]}} [r, g, b] values 0–255
 */
function hexToRgb(hex) {{
  const r = parseInt(hex.slice(1,3), 16);  // Characters 1–2 = red
  const g = parseInt(hex.slice(3,5), 16);  // Characters 3–4 = green
  const b = parseInt(hex.slice(5,7), 16);  // Characters 5–6 = blue
  return [r, g, b];
}}

// ════════════════════════════════════════════════════════════════════════════
// RECLOSER MARKER RENDERING
// ════════════════════════════════════════════════════════════════════════════

/**
 * buildRecloserMarkers()
 * Clear and redraw all recloser dots on the map.
 * Called on initial load and whenever the user changes the min-click slider
 * or dot-size slider (which update minClicks and dotRadius before calling this).
 *
 * Each recloser becomes a Leaflet circleMarker:
 *   - Position  : [r.lat, r.lon]
 *   - Fill colour: r.color (pre-computed green→red hex from Python)
 *   - Radius    : dotRadius (pixels, controlled by slider)
 *   - Border    : semi-transparent version of the fill colour
 *   - Border weight: 2px for high-severity reclosers (score > 0.7), else 1px
 *
 * DEBUG: If no dots appear, check:
 *   1. That RECLOSERS.length > 0 in the browser console
 *   2. That r.lat/r.lon values are within ~33–35°N, 83–85°W
 *   3. That minClicks hasn't been set higher than all totalClicks values
 */
function buildRecloserMarkers() {{
  // Remove all existing markers from the layer group before rebuilding
  layers.reclosers.clearLayers();

  RECLOSERS.forEach(r => {{
    // Skip reclosers that don't meet the current minimum click threshold
    if (r.totalClicks < minClicks) return;

    // Parse the hex colour so we can make a transparent border version of it
    const [rv, gv, bv] = hexToRgb(r.color);

    const marker = L.circleMarker([r.lat, r.lon], {{
      radius:      dotRadius,
      fillColor:   r.color,
      color:       `rgba(${{rv}},${{gv}},${{bv}},0.4)`,  // 40% opacity ring
      weight:      r.score > 0.7 ? 2 : 1,  // Thicker ring for high-severity
      fillOpacity: 0.85,
    }});

    // Attach a popup that shows basic info and has an expandable advanced section
    marker.bindPopup(buildRecloserPopup(r), {{ maxWidth: 320 }});
    marker.addTo(layers.reclosers);
  }});
}}

/**
 * buildRecloserPopup(r)
 * Build and return the HTML string for a recloser's click popup.
 * The popup has two sections:
 *   - Basic: always visible (severity badge, total clicks, event count)
 *   - Advanced: hidden by default, revealed by the "Show Advanced" button
 *
 * The advanced section is a <div> with id="adv-{{repId}}" — toggled by toggleAdv().
 *
 * @param {{object}} r - A recloser object from the RECLOSERS array
 * @returns {{string}} HTML string passed to marker.bindPopup()
 *
 * DEBUG: If popup content looks wrong, add console.log(r) at the top of this
 *        function to inspect the full data object for the clicked recloser.
 */
function buildRecloserPopup(r) {{
  // Choose a badge based on score thresholds:
  //   ≥ 0.70 = HIGH (red),  0.35–0.69 = MED (yellow),  < 0.35 = LOW (green)
  const scoreLabel = r.score > 0.7
    ? '<span class="badge badge-red">HIGH</span>'
    : r.score > 0.35
      ? '<span class="badge badge-yellow">MED</span>'
      : '<span class="badge badge-green">LOW</span>';

  return `
    <div class="popup-title">⚡ RECLOSER ${{r.repId}}</div>
    <!-- Basic info — always visible -->
    <div class="popup-row">
      <span class="popup-key">Status</span>
      <span class="popup-val">${{scoreLabel}}</span>
    </div>
    <div class="popup-row">
      <span class="popup-key">Total Clicks</span>
      <span class="popup-val">${{r.totalClicks}}</span>
    </div>
    <div class="popup-row">
      <span class="popup-key">Event Windows</span>
      <span class="popup-val">${{r.eventCount}}</span>
    </div>
    <!-- Advanced section — hidden until toggleAdv() is called -->
    <div class="popup-advanced" id="adv-${{r.repId}}">
      <div class="popup-row">
        <span class="popup-key">Avg Clicks/Window</span>
        <span class="popup-val">${{r.avgClicks}}</span>
      </div>
      <div class="popup-row">
        <span class="popup-key">Peak Window</span>
        <span class="popup-val">${{r.maxWindow}}</span>
      </div>
      <div class="popup-row">
        <span class="popup-key">First Seen</span>
        <span class="popup-val">${{r.firstSeen}}</span>
      </div>
      <div class="popup-row">
        <span class="popup-key">Last Seen</span>
        <span class="popup-val">${{r.lastSeen}}</span>
      </div>
      <div class="popup-row">
        <span class="popup-key">Click Score</span>
        <span class="popup-val">${{(r.score * 100).toFixed(1)}}%</span>
      </div>
    </div>
    <!-- Button passes the repId and the button element itself to toggleAdv() -->
    <button class="detail-toggle" onclick="toggleAdv('${{r.repId}}', this)">▼ Show Advanced</button>
  `;
}}

// ════════════════════════════════════════════════════════════════════════════
// SUBSTATION MARKER RENDERING
// ════════════════════════════════════════════════════════════════════════════

/**
 * buildSubstationMarkers()
 * Clear and redraw all substation rings on the map.
 * Called on initial load and when the district filter dropdown changes.
 *
 * Each substation is a cyan circle where:
 *   - Radius grows with outage count (capped at 28px so large substations
 *     don't overwhelm the map): radius = min(10 + outageCount × 0.8, 28)
 *   - Fill opacity increases with intensity (more outages = more opaque):
 *     fillOpacity = 0.25 + (outageCount / 20) × 0.4
 *
 * DEBUG: If substation rings appear in the wrong district, the district_offset()
 *        Python function may be producing coordinates outside the expected range.
 *        Print sub_agg[["Sub","District","lat","lon"]] in Python to verify.
 */
function buildSubstationMarkers() {{
  layers.substations.clearLayers();

  SUBSTATIONS.forEach(s => {{
    // If a district filter is active and this substation isn't in it, skip
    if (districtFilter !== 'all' && s.district !== districtFilter) return;

    // Scale opacity and size by outage count
    const intensity = Math.min(1, s.outageCount / 20);  // Cap at 1.0 (20+ outages = max)
    const size = 10 + s.outageCount * 0.8;

    const marker = L.circleMarker([s.lat, s.lon], {{
      radius:      Math.min(size, 28),
      fillColor:   '#00c8ff',                           // Cyan — distinct from reclosers
      color:       'rgba(0,200,255,0.3)',               // Semi-transparent cyan border
      weight:      1.5,
      fillOpacity: 0.25 + intensity * 0.4,
    }});

    marker.bindPopup(buildSubstationPopup(s), {{ maxWidth: 320 }});
    marker.addTo(layers.substations);
  }});
}}

/**
 * buildSubstationPopup(s)
 * Build the HTML popup for a substation marker.
 * Includes basic stats (always visible) and a collapsible "Top Causes" table.
 *
 * The causes object is {{cause_description: count, ...}} — we iterate its entries
 * to build a row for each cause.
 *
 * @param {{object}} s - A substation object from the SUBSTATIONS array
 * @returns {{string}} HTML string for marker.bindPopup()
 *
 * DEBUG: If "Show Causes" reveals no rows, the causes dict may be empty.
 *        Check sub_agg["causes"] in Python to see if Cause Desc data is present.
 */
function buildSubstationPopup(s) {{
  // Build a table row for each top-3 cause (or show a fallback message)
  const causeRows = Object.entries(s.causes || {{}})
    .map(([k, v]) => `
      <div class="popup-row">
        <span class="popup-key" style="font-size:10px">${{k.slice(0, 28)}}</span>
        <span class="popup-val">${{v}}x</span>
      </div>`)
    .join('');

  // The substation name may contain spaces so we replace them with hyphens
  // when building the HTML id for the advanced div — spaces are invalid in ids.
  const safeId = s.sub.replace(/\\s/g, '-');

  return `
    <div class="popup-title">🏭 ${{s.sub}}</div>
    <!-- Basic info -->
    <div class="popup-row">
      <span class="popup-key">District</span>
      <span class="popup-val">${{s.district}}</span>
    </div>
    <div class="popup-row">
      <span class="popup-key">Outages</span>
      <span class="popup-val">${{s.outageCount}}</span>
    </div>
    <div class="popup-row">
      <span class="popup-key">Customers Affected</span>
      <span class="popup-val">${{s.customersAffected}}</span>
    </div>
    <div class="popup-row">
      <span class="popup-key">Customer·Minutes</span>
      <span class="popup-val">${{s.customerMinutes.toLocaleString()}}</span>
    </div>
    <div class="popup-row">
      <span class="popup-key">Avg Duration (hrs)</span>
      <span class="popup-val">${{s.avgDuration}}</span>
    </div>
    <!-- Collapsible causes section -->
    <div class="popup-advanced" id="adv-sub-${{safeId}}">
      <div style="font-size:10px;color:var(--muted);margin-bottom:4px;
                  font-family:'Share Tech Mono',monospace">TOP CAUSES</div>
      ${{causeRows || '<div style="color:var(--muted);font-size:11px">No cause data</div>'}}
    </div>
    <button class="detail-toggle" onclick="toggleAdv('sub-${{safeId}}', this)">▼ Show Causes</button>
  `;
}}

// ════════════════════════════════════════════════════════════════════════════
// HEATMAP RENDERING
// ════════════════════════════════════════════════════════════════════════════

/**
 * buildClickHeatmap()
 * (Re)build the click-count heatmap layer using leaflet.heat.
 * Points: [lat, lon, intensity] where intensity = totalClicks.
 * The heatmap auto-normalises intensities so the highest-click recloser
 * always appears red regardless of absolute value.
 *
 * Called on initial load (heatmap starts hidden) and whenever the min-click
 * slider changes (to keep the heatmap in sync with the dot filter).
 *
 * Heatmap parameters:
 *   radius  – size of each point's influence circle (pixels at default zoom)
 *   blur    – Gaussian blur applied to the heat layer (higher = smoother)
 *   maxZoom – the zoom level at which point spread equals radius
 *   gradient – colour stops from 0.0 (cool) to 1.0 (hot)
 *
 * DEBUG: If the heatmap doesn't appear after toggling, check that
 *        document.getElementById('tog-heatmap').checked is true, and that
 *        layers.heatmap was successfully added to map.
 */
function buildClickHeatmap() {{
  // Remove the old heatmap layer before creating a new one
  if (layers.heatmap) map.removeLayer(layers.heatmap);

  // Build array of [lat, lon, weight] tuples, respecting the current min filter
  const pts = RECLOSERS
    .filter(r => r.totalClicks >= minClicks)
    .map(r => [r.lat, r.lon, r.totalClicks]);

  layers.heatmap = L.heatLayer(pts, {{
    radius:   35,
    blur:     25,
    maxZoom:  12,
    gradient: {{
      0.0: '#00ff00',  // Green = low activity
      0.4: '#ffff00',  // Yellow = moderate
      0.7: '#ff8800',  // Orange = high
      1.0: '#ff0000',  // Red = maximum
    }},
  }});

  // Only add to map if the toggle is currently checked
  if (document.getElementById('tog-heatmap').checked) {{
    layers.heatmap.addTo(map);
  }}
}}

/**
 * buildOutageHeatmap()
 * Build the outage-severity heatmap layer.
 * Uses customer-minutes as the intensity weight — this reflects both the
 * number of customers affected AND the duration, making it a good proxy
 * for total impact.
 *
 * Weight is divided by 100 to scale down large customer-minute values
 * into a range that leaflet.heat can distinguish between.
 *
 * Uses a blue→purple gradient to distinguish it visually from the green→red
 * click heatmap so both can be enabled simultaneously.
 */
function buildOutageHeatmap() {{
  if (layers.outageheat) map.removeLayer(layers.outageheat);

  // Weight by customer-minutes for outage severity
  const pts = SUBSTATIONS.map(s => [s.lat, s.lon, s.customerMinutes / 100]);

  layers.outageheat = L.heatLayer(pts, {{
    radius:   45,
    blur:     30,
    maxZoom:  12,
    gradient: {{
      0.0: '#0000ff',  // Blue = low impact
      0.4: '#8800ff',  // Purple = moderate
      1.0: '#ff00ff',  // Magenta = maximum impact
    }},
  }});

  if (document.getElementById('tog-outageheat').checked) {{
    layers.outageheat.addTo(map);
  }}
}}

// ════════════════════════════════════════════════════════════════════════════
// UI CONTROL CALLBACKS
// These functions are called directly from HTML element event handlers
// (onchange, oninput, onclick attributes in the sidebar markup above).
// ════════════════════════════════════════════════════════════════════════════

/**
 * toggleLayer(name, show)
 * Show or hide a named map layer.
 * The heatmap layers require special handling because they're rebuilt as
 * new L.heatLayer instances; LayerGroup objects use addTo/removeLayer directly.
 *
 * @param {{string}} name  - 'reclosers', 'substations', 'heatmap', or 'outageheat'
 * @param {{boolean}} show - true = add to map, false = remove from map
 */
function toggleLayer(name, show) {{
  if (name === 'heatmap') {{
    // layers.heatmap could be null if it was never built yet
    if (show) {{ layers.heatmap && layers.heatmap.addTo(map); }}
    else      {{ layers.heatmap && map.removeLayer(layers.heatmap); }}
  }} else if (name === 'outageheat') {{
    if (show) {{ layers.outageheat && layers.outageheat.addTo(map); }}
    else      {{ layers.outageheat && map.removeLayer(layers.outageheat); }}
  }} else {{
    // Standard LayerGroup — add or remove directly
    if (show) layers[name].addTo(map);
    else      map.removeLayer(layers[name]);
  }}
}}

/**
 * filterMinClicks(v)
 * Called when the "Min Click Count" slider moves.
 * Updates the global minClicks threshold, refreshes the readout label,
 * then rebuilds both the recloser dots and click heatmap so they stay in sync.
 *
 * @param {{number}} v - new minimum value from the slider
 */
function filterMinClicks(v) {{
  minClicks = v;
  document.getElementById('min-click-val').textContent = v;  // Update display
  buildRecloserMarkers();   // Redraw dots respecting new threshold
  buildClickHeatmap();      // Redraw heatmap respecting new threshold
}}

/**
 * updateDotSize(v)
 * Called when the "Dot Size" slider moves.
 * Updates dotRadius and redraws all recloser markers at the new size.
 *
 * @param {{number}} v - new radius in pixels
 */
function updateDotSize(v) {{
  dotRadius = v;
  document.getElementById('dot-size-val').textContent = v;
  buildRecloserMarkers();
}}

/**
 * filterDistrict(v)
 * Called when the district dropdown selection changes.
 * Updates districtFilter and redraws substation rings (which check this value).
 * Recloser dots are unaffected — they have no district metadata in this dataset.
 *
 * @param {{string}} v - district name or 'all'
 */
function filterDistrict(v) {{
  districtFilter = v;
  buildSubstationMarkers();
}}

/**
 * toggleAdv(id, btn)
 * Show or hide the "advanced" section inside a popup.
 * The advanced div has id="adv-{id}" — we toggle the CSS class "show"
 * which switches its display from none to block.
 * The button label is also updated to match the current state.
 *
 * @param {{string}} id  - the suffix used in the advanced div's id attribute
 * @param {{HTMLElement}} btn - the button element (to update its label text)
 */
function toggleAdv(id, btn) {{
  const el = document.getElementById('adv-' + id);
  if (!el) return;  // Guard against missing element (shouldn't happen normally)
  el.classList.toggle('show');
  btn.textContent = el.classList.contains('show') ? '▲ Hide Advanced' : '▼ Show Advanced';
}}

// ════════════════════════════════════════════════════════════════════════════
// SIDEBAR LIST BUILDERS
// These populate the Reclosers, Outages, and Flagged tab panes with
// clickable cards.  Clicking a card calls flyTo() to pan/zoom the map.
// ════════════════════════════════════════════════════════════════════════════

/**
 * buildRecloserList()
 * Populate the "Reclosers" tab with cards sorted by total clicks (highest first).
 * Each card shows the severity badge, RepId, total clicks, and event window count.
 * Clicking a card calls flyTo() to centre the map on that recloser at zoom 14.
 */
function buildRecloserList() {{
  // Sort a copy of RECLOSERS descending by totalClicks
  const sorted = [...RECLOSERS].sort((a, b) => b.totalClicks - a.totalClicks);

  const el = document.getElementById('recloser-list');
  el.innerHTML = sorted.map(r => {{
    const badge = r.score > 0.7
      ? '<span class="badge badge-red">HIGH</span>'
      : r.score > 0.35
        ? '<span class="badge badge-yellow">MED</span>'
        : '<span class="badge badge-green">LOW</span>';

    // onclick: fly the map to this recloser's position at zoom 14
    return `
      <div class="info-item" onclick="flyTo(${{r.lat}}, ${{r.lon}}, 14)">
        <div class="iname">${{badge}} Rep ${{r.repId}}</div>
        <div class="imeta">Clicks: ${{r.totalClicks}} | Windows: ${{r.eventCount}}</div>
      </div>`;
  }}).join('');
}}

/**
 * buildOutageList()
 * Populate the "Outages" tab with substation cards sorted by outage count.
 * Clicking a card pans to the substation at zoom 13.
 */
function buildOutageList() {{
  const sorted = [...SUBSTATIONS].sort((a, b) => b.outageCount - a.outageCount);

  const el = document.getElementById('outage-list');
  el.innerHTML = sorted.map(s => `
    <div class="info-item" onclick="flyTo(${{s.lat}}, ${{s.lon}}, 13)">
      <div class="iname">🏭 ${{s.sub}}</div>
      <div class="imeta">${{s.district}} | ${{s.outageCount}} outages | ${{s.customersAffected}} cust.</div>
    </div>`).join('');
}}

/**
 * buildFlaggedList()
 * Populate the "Flagged" tab with two sections:
 *   1. Flagged outage rows (from FLAGGED_OUT)
 *   2. Flagged click-count rows (from FLAGGED_CLK)
 *
 * Each card shows the record identifier, the list of missing column names,
 * and any available context (date for outages, source sheet for clicks).
 *
 * This gives data stewards a clear list of records to investigate and fix
 * in the source Excel workbook.
 */
function buildFlaggedList() {{
  const el       = document.getElementById('flagged-list');
  const countEl  = document.getElementById('flagged-count');
  const total    = FLAGGED_OUT.length + FLAGGED_CLK.length;

  countEl.textContent = `${{total}} entries flagged and excluded from analysis`;

  let html = '';

  // Outage flagged section
  if (FLAGGED_OUT.length) {{
    html += `<div class="control-label" style="margin-bottom:6px">
               OUTAGE DATA (${{FLAGGED_OUT.length}})
             </div>`;
    html += FLAGGED_OUT.map(f => `
      <div class="flagged-item">
        <div class="ftitle">Outage #${{f.outage}}</div>
        <div class="fmissing">Missing: ${{f.missing.join(', ')}}</div>
        <div class="fmissing">Date: ${{f.timeOff}}</div>
      </div>`).join('');
  }}

  // Click-count flagged section
  if (FLAGGED_CLK.length) {{
    html += `<div class="control-label" style="margin:10px 0 6px">
               CLICK COUNT DATA (${{FLAGGED_CLK.length}})
             </div>`;
    html += FLAGGED_CLK.map(f => `
      <div class="flagged-item">
        <div class="ftitle">RepId: ${{f.repId}}</div>
        <div class="fmissing">Missing: ${{f.missing.join(', ')}}</div>
        <div class="fmissing">Sheet: ${{f.sheet}}</div>
      </div>`).join('');
  }}

  el.innerHTML = html;
}}

// ════════════════════════════════════════════════════════════════════════════
// MAP NAVIGATION HELPER
// ════════════════════════════════════════════════════════════════════════════

/**
 * flyTo(lat, lon, zoom)
 * Smoothly pan and zoom the Leaflet map to a given position.
 * Used by sidebar list item click handlers.
 * duration: 1 second animation — increase for slower pan, decrease for faster.
 *
 * @param {{number}} lat  - target latitude
 * @param {{number}} lon  - target longitude
 * @param {{number}} zoom - target zoom level (14 = street, 13 = town, 9 = region)
 */
function flyTo(lat, lon, zoom) {{
  map.flyTo([lat, lon], zoom, {{ duration: 1 }});
}}

// ════════════════════════════════════════════════════════════════════════════
// TAB SWITCHING
// ════════════════════════════════════════════════════════════════════════════

/**
 * switchTab(name)
 * Activate the sidebar tab matching the given name and deactivate all others.
 * Works by matching tab button order (0–3) to a hardcoded name array.
 *
 * @param {{string}} name - 'controls', 'reclosers', 'outages', or 'flagged'
 *
 * DEBUG: If tabs don't switch, verify the button order in HTML matches the
 *        names array below, and that each tab pane id matches 'tab-' + name.
 */
function switchTab(name) {{
  const names = ['controls', 'reclosers', 'outages', 'flagged'];

  // Toggle .active on tab buttons by matching their position to the names array
  document.querySelectorAll('.tab-btn').forEach((btn, i) => {{
    btn.classList.toggle('active', names[i] === name);
  }});

  // Toggle .active on tab panes by matching their id to 'tab-' + name
  document.querySelectorAll('.tab-pane').forEach(pane => {{
    pane.classList.toggle('active', pane.id === 'tab-' + name);
  }});
}}

// ════════════════════════════════════════════════════════════════════════════
// INITIALISATION — runs once when the page loads
// The order matters: data must be prepared before anything is drawn.
// ════════════════════════════════════════════════════════════════════════════

buildRecloserMarkers();   // Step 1: Draw recloser dots (data layer)
buildSubstationMarkers(); // Step 2: Draw substation rings (data layer)
buildClickHeatmap();      // Step 3: Pre-build click heatmap (starts hidden)
buildOutageHeatmap();     // Step 4: Pre-build outage heatmap (starts hidden)
buildRecloserList();      // Step 5: Populate Reclosers sidebar tab
buildOutageList();        // Step 6: Populate Outages sidebar tab
buildFlaggedList();       // Step 7: Populate Flagged sidebar tab

</script>
</body>
</html>"""
    return html


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 7 — ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

def main():
    """
    Orchestrate the full pipeline:
        load_data → aggregate_data → build_html → write file

    Accepts optional command-line arguments:
        argv[1] – path to the input .xlsx file (default: EXCEL_PATH)
        argv[2] – path for the output .html file (default: OUTPUT_HTML)

    Example:
        python3 generate_map.py                          # use defaults
        python3 generate_map.py my_data.xlsx             # custom input
        python3 generate_map.py my_data.xlsx report.html # custom in + out

    DEBUG:
        • Wrap any of the four function calls in a try/except to isolate which
          stage is failing, e.g.:
              try:
                  outage, flagged_outages, clicks, flagged_clicks = load_data(path)
              except Exception as e:
                  print(f"load_data failed: {e}")
                  import traceback; traceback.print_exc()
        • Add  print(sub_agg.dtypes)  or  print(rep_agg.head())  after
          aggregate_data() to verify the summary DataFrames look correct before
          they're serialised into the HTML.
    """
    # Read optional command-line arguments, falling back to the config defaults
    path   = sys.argv[1] if len(sys.argv) > 1 else EXCEL_PATH
    output = sys.argv[2] if len(sys.argv) > 2 else OUTPUT_HTML

    print(f"\n{'='*55}")
    print(" Georgia Outage & Click Count Map Generator")
    print(f"{'='*55}")
    print(f"Input:  {path}")
    print(f"Output: {output}\n")

    # ── STEP 1: Load and validate all data from the workbook ────────────────
    outage, flagged_outages, clicks, flagged_clicks = load_data(path)

    # ── STEP 2: Aggregate to per-substation and per-recloser summaries ───────
    sub_agg, rep_agg = aggregate_data(outage, clicks)

    # ── STEP 3: Build the complete HTML string ───────────────────────────────
    html = build_html(sub_agg, rep_agg, flagged_outages, flagged_clicks)

    # ── STEP 4: Write the HTML to disk ───────────────────────────────────────
    # encoding="utf-8" ensures special characters (like ·) render correctly
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(output) // 1024
    print(f"\n✓ Map written to: {output} ({size_kb} KB)")
    print(f"  Open in any browser to explore the interactive map.\n")


# Standard Python idiom: only run main() when this file is executed directly,
# not when it's imported as a module by another script.
if __name__ == "__main__":
    main()