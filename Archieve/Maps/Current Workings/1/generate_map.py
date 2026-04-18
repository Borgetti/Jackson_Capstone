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
import csv       # Used by read_STD_file() and load_mpt_network() to parse CSVs
import math      # Used by lcc_to_latlon() for trigonometric projection math
import json      # Used to serialise Python dicts/lists into JavaScript arrays
import sys       # Used for sys.argv (command-line arguments) and sys.exit()
import os        # Used to check that the input file exists before opening it
import hashlib   # Used to generate deterministic (repeatable) fake coordinates
                 # from device IDs -- same ID always gets the same map position
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
#   EXCEL_PATH      -- path to the workbook
#   OUTPUT_HTML     -- name/path of the HTML file that will be created
#   MPT_PATH        -- path to the WindMil .mpt GIS export (or None to skip)
#   STD_PATH        -- path to the WindMil .std network topology file (or None)
#   MAX_SPANS       -- how many network spans to embed (None = all ~149k)
#   DISTRICT_COORDS -- lat/lon centre points for each district (fallback only)
# ─────────────────────────────────────────────────────────────────────────────

# Default input file -- override via command-line argument (see main() below)
EXCEL_PATH = "Click_Count_and_Outage_Management_Data.xlsx"

# Default output file -- will be created (or overwritten) in the current directory
OUTPUT_HTML = "outage_map.html"

# Path to the Milsoft WindMil MPT file (GIS map-points export).
# Format: span_name, X_easting (State Plane feet), Y_northing (State Plane feet)
# Set to None to skip drawing network lines.
MPT_PATH = "gs12083_windmil_export_09_15_2025.mpt"

# Path to the Milsoft WindMil STD file (network topology CSV).
# This is the primary source of REAL coordinates for:
#   - Substations     (element type 9  -- SOURCE)
#   - Overcurrent devices / reclosers  (element type 10 -- OCDEV)
#   - Switches        (element type 6  -- SWITCH)
#   - Transformers    (element type 5  -- TRANS)
# When provided, the STD is parsed and element names are joined to the outage
# Excel data on "Sub" (-> type 9) and "Next Pro Dvc" (-> type 10 / 6).
# Any element found in the STD is plotted at its real surveyed coordinates.
# Any element NOT found in the STD falls back to the MD5-hash approximation.
# Set to None to skip STD loading (all coordinates will be approximated).
STD_PATH = "gs12083_windmil_export_09_15_2025_bad.std"

# Maximum number of spans to embed in the HTML.
# The full MPT has ~149,000 spans; embedding all produces ~8 MB of extra JS data.
# 50,000 spans covers the core service territory and keeps the file manageable.
# Set to None to embed all spans.
MAX_SPANS = None

# Geographic centre of each district in NE Georgia (latitude, longitude).
# Used ONLY as a fallback when a substation cannot be found in the STD file.
# Once the full STD is available these are no longer needed for placement.
DISTRICT_COORDS = {
    "Gainesville":    (34.2979, -83.8241),
    "Jefferson":      (34.1123, -83.5999),
    "Lawrenceville":  (33.9566, -83.9880),
    "Neese":          (34.1500, -83.9000),
}

# ── STD file column indices ────────────────────────────────────────────────────
# The STD file is a comma-delimited CSV.  Columns are 0-indexed.
# Source: Chapter 15, Milsoft Layout documentation (Table 15.1 Standard Fields
# plus element-specific field tables for each type).
#
# Standard fields present on EVERY element row:
STD_NAME       = 0   # Element Name  -- unique alphanumeric ID, max 32 characters
STD_ETYPE      = 1   # Element Type  -- integer code; see STD_TYPE_* constants below
STD_PHASE      = 2   # Phase configuration -- see STD_PH_* constants below
STD_PARENT     = 3   # Parent Element Name (the element directly upstream)
STD_MAP        = 4   # Map Number (user-definable; not used by this script)
STD_XL         = 5   # X Coordinate downline / load-side endpoint (State Plane ft)
STD_YL         = 6   # Y Coordinate downline / load-side endpoint (State Plane ft)
STD_USERTAG    = 7   # User Tag (free-text field; not used by this script)
# Element-specific fields that appear further along the row:
STD_GUID       = 49  # Element GUID  (globally unique identifier, export-only)
STD_PARENTGUID = 50  # Parent Element GUID
#
# OH/UG span fields (element types 1 and 3) -- field IDs L32/L33 in the spec:
STD_XS         = 31  # X Coordinate upline / source-side endpoint (State Plane ft)
STD_YS         = 32  # Y Coordinate upline / source-side endpoint (State Plane ft)
#
# Node fields (element type 8) -- field IDs F28/F29/F30 in the spec:
STD_PARENTA    = 27  # A-Phase parent element name
STD_PARENTB    = 28  # B-Phase parent element name
STD_PARENTC    = 29  # C-Phase parent element name
STD_ISMULTIPAR = 30  # IsMultiParent flag (0 = single parent, 1 = multi-parent)
#
# Overcurrent device fields (element type 10) -- field IDs O12/O13/O14 in the spec:
STD_OSTATUSA   = 11  # Is Closed, Phase A  (0 = open/dead, 1 = closed, 2 = bypassed)
STD_OSTATUSB   = 12  # Is Closed, Phase B
STD_OSTATUSC   = 13  # Is Closed, Phase C
#
# Switch fields (element type 6) -- field IDs E9/E10/E11 in the spec:
STD_SWSTATUS   = 8   # Switch Status ('C' = closed, 'O' = open)
STD_SWID       = 9   # Switch ID (up to 15 characters)
STD_SWPARTNER  = 10  # Partner Identifier -- name of the other half of the switch pair

# ── STD element type codes ─────────────────────────────────────────────────────
# The value in column STD_ETYPE tells you what kind of network element the row is.
# Source: Chapter 15, Milsoft Layout documentation, Section 15.1.1.
STD_TYPE_OHSPAN = '1'   # Overhead line span (conductor between two poles/towers)
STD_TYPE_CAP    = '2'   # Capacitor bank
STD_TYPE_UGSPAN = '3'   # Underground cable span
STD_TYPE_REG    = '4'   # Voltage regulator
STD_TYPE_TRANS  = '5'   # Transformer
STD_TYPE_SWITCH = '6'   # Switch (sectionalizer, tie switch, etc.)
STD_TYPE_NODE   = '8'   # Node (load / junction point; no type 7 in the spec)
STD_TYPE_SOURCE = '9'   # Source (substation feed point -- head of a feeder)
STD_TYPE_OCDEV  = '10'  # Overcurrent device (fuse, recloser, sectionalizer)
STD_TYPE_MOTOR  = '11'  # Motor
STD_TYPE_GEN    = '12'  # Generator
STD_TYPE_CONS   = '13'  # Consumer (exported as node; see spec note in §15.13)

# ── STD phase configuration codes ─────────────────────────────────────────────
# The value in column STD_PHASE encodes which phases are present on the element.
# Source: Chapter 17, §17.1.7.19.
STD_PH_A   = '1'   # Phase A only
STD_PH_B   = '2'   # Phase B only
STD_PH_C   = '3'   # Phase C only
STD_PH_AB  = '4'   # Phases A and B
STD_PH_AC  = '5'   # Phases A and C
STD_PH_BC  = '6'   # Phases B and C
STD_PH_ABC = '7'   # All three phases (three-phase element)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2 — COORDINATE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def lcc_to_latlon(E: float, N: float) -> tuple:
    """
    Convert Georgia West State Plane coordinates (EPSG:2240, US Survey Feet,
    NAD83) to WGS84 geographic coordinates (decimal degrees).

    Uses a full Lambert Conformal Conic (LCC) inverse projection so that no
    external library (e.g. pyproj) is needed.  The projection parameters are
    fixed for Georgia West SPCS:
        Latitude of origin:         30 deg 00' N
        Central meridian:           84 deg 10' W
        Standard parallel 1:        31 deg 46' N
        Standard parallel 2:        34 deg 20' N
        False Easting:              2,296,583.333 US Survey Feet
        False Northing:             0

    PARAMETERS:
        E  --  Easting  in US Survey Feet (the X column in the MPT file)
        N  --  Northing in US Survey Feet (the Y column in the MPT file)

    RETURNS:
        (lat, lon) tuple in decimal degrees, rounded to 5 decimal places
        (~1.1 m precision, more than enough for a map at zoom 9-17)

    DEBUG:
        A correct result for the NE Georgia service area should give latitudes
        in the range 33.7 -- 34.7 and longitudes in the range -84.4 -- -83.2.
        If you see very different values, the input may not be in GA West SPCS.
        Try GA East SPCS (EPSG:2239, central meridian -82 deg 10') instead.
    """
    # GRS80 ellipsoid parameters (NAD83 uses GRS80)
    a    = 20925604.0472   # Semi-major axis in US Survey Feet
    f    = 1.0 / 298.257222101
    e2   = 2*f - f**2
    e    = math.sqrt(e2)

    # Projection parameters for Georgia West State Plane
    phi0 = math.radians(30.0)                   # Latitude of origin
    lam0 = math.radians(-84.0 - 10.0/60.0)     # Central meridian
    phi1 = math.radians(31.0 + 46.0/60.0)      # Standard parallel 1
    phi2 = math.radians(34.0 + 20.0/60.0)      # Standard parallel 2
    E0   = 2296583.333                          # False Easting
    N0   = 0.0                                  # False Northing

    # Helper: m(phi) = cos(phi) / sqrt(1 - e^2 sin^2(phi))
    def m_func(phi):
        return math.cos(phi) / math.sqrt(1 - e2 * math.sin(phi)**2)

    # Helper: t(phi) = tan(pi/4 - phi/2) / ((1-e sin phi)/(1+e sin phi))^(e/2)
    def t_func(phi):
        sp = math.sin(phi)
        return math.tan(math.pi/4 - phi/2) / ((1 - e*sp) / (1 + e*sp))**(e/2)

    m1  = m_func(phi1);  m2  = m_func(phi2)
    t0  = t_func(phi0);  t1  = t_func(phi1);  t2  = t_func(phi2)

    # Cone constant and scale factor
    n   = math.log(m1 / m2) / math.log(t1 / t2)
    F   = m1 / (n * t1**n)
    r0  = a * F * t0**n   # Radius to origin latitude

    # Inverse: compute r and theta from the grid coordinates
    r = math.sqrt((E - E0)**2 + (r0 - (N - N0))**2)
    if n < 0:
        r = -r
    theta = math.atan2(E - E0, r0 - (N - N0))

    # t value from r, then iterate to get phi (latitude)
    t_val = (r / (a * F)) ** (1.0 / n)
    phi   = math.pi/2 - 2 * math.atan(t_val)
    for _ in range(10):    # 3-4 iterations converge to full double precision
        sp   = math.sin(phi)
        phi  = math.pi/2 - 2 * math.atan(t_val * ((1 - e*sp) / (1 + e*sp))**(e/2))

    # Longitude from theta
    lam = theta / n + lam0

    return (round(math.degrees(phi), 5), round(math.degrees(lam), 5))


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2.5 — STD FILE READER  (adapted from windmilfixer.py)
# ════════════════════════════════════════════════════════════════════════════════

def read_STD_file(file_path: str):
    """
    Read a Milsoft WindMil STD CSV file into a list of rows with sorted search
    indices.  Adapted from windmilfixer.py's read_STD_file().

    The STD file is a comma-delimited ASCII file where each row describes one
    network element.  See Chapter 15 of the Milsoft Layout documentation for the
    full field specification.  Key columns (0-indexed):
        0  -- Element Name  (unique alphanumeric ID)
        1  -- Element Type  ('1'=OH span, '3'=UG span, '6'=switch, '8'=node,
                             '9'=source, '10'=overcurrent device)
        2  -- Phase configuration code
        3  -- Parent Element Name
        5  -- X Coordinate downline (load end, State Plane US Survey Feet)
        6  -- Y Coordinate downline
        49 -- Element GUID
        50 -- Parent Element GUID

    PARAMETERS:
        file_path  --  Full path to the .std file

    RETURNS:
        data          -- list of rows; each row is a list of column strings
        name_index    -- list of row indices sorted by data[i][STD_NAME]
                         Used with find_STD_record() for O(log n) name lookups
        x_index       -- list of row indices sorted by data[i][STD_XL]
                         Used for spatial proximity searches
        parent_index  -- list of row indices sorted by data[i][STD_PARENT]

    NOTES:
        The returned data list is 0-indexed.  A sorted index maps a sorted
        position back to the original row number:
            sorted_name_index[k] = i  means  data[i] is the k-th name in sort order
        Use find_STD_record() (below) to do a binary search on any of these indices.

    DEBUG:
        If you get a KeyError or IndexError, the file may have a header line
        ("MILSOFT STD WM ASCII,001").  The reader skips blank lines but not the
        header -- strip it manually or add a check for row[0] == "MILSOFT STD WM ASCII".
    """
    with open(file_path, 'r') as fh:
        reader = csv.reader(fh)
        data   = list(reader)

    # Skip any empty rows (can appear at end of file)
    data = [row for row in data if len(row) > STD_ETYPE]

    # Build sorted index arrays -- each is a permutation of range(len(data))
    # sorted by the corresponding column value.
    # Lambda functions capture the column constant (not a closure over a loop var).
    name_index   = sorted(range(len(data)), key=lambda i: data[i][STD_NAME])
    x_index      = sorted(range(len(data)), key=lambda i: data[i][STD_XL])
    parent_index = sorted(range(len(data)), key=lambda i: data[i][STD_PARENT])

    return data, name_index, x_index, parent_index


def find_STD_record(data: list, sorted_index: list, search_value: str, col: int):
    """
    Binary-search the STD data array for a row whose column[col] == search_value.
    Adapted from windmilfixer.py's find_record().

    Because multiple rows can share the same value (e.g. multiple elements with
    the same parent), the function finds the FIRST match in sorted order.

    PARAMETERS:
        data          -- the data list returned by read_STD_file()
        sorted_index  -- the matching index (name_index, x_index, or parent_index)
        search_value  -- the string to look for
        col           -- column index to compare against (e.g. STD_NAME, STD_XL)

    RETURNS:
        (record, row_idx, sorted_pos) where:
          record     -- the matching row as a list of strings, or None if not found
          row_idx    -- absolute index into data[]
          sorted_pos -- position in sorted_index[] (useful for iterating neighbours)

    NOTES:
        When record is None, row_idx and sorted_pos point to the nearest value --
        useful for walking the sorted list to find nearby elements.
    """
    left  = 0
    right = len(sorted_index) - 1

    while left <= right:
        mid = (left + right) // 2
        idx = sorted_index[mid]
        val = data[idx][col]

        if val == search_value:
            # Rewind to the first occurrence of this value
            while mid > 0 and data[sorted_index[mid - 1]][col] == search_value:
                mid -= 1
            idx = sorted_index[mid]
            return data[idx], idx, mid

        elif val < search_value:
            left  = mid + 1
        else:
            right = mid - 1

    # Not found -- return the closest position so callers can walk from here
    return None, sorted_index[mid] if sorted_index else 0, mid


def load_STD_elements(std_path: str) -> dict:
    """
    Parse the WindMil STD file and return a flat name-keyed lookup of every
    network element that has a usable coordinate — ready to join against the
    outage Excel data.

    The STD is a comma-delimited CSV (Chapter 15, Milsoft Layout documentation).
    This function reads every row, converts the XL/YL State Plane coordinates to
    WGS84 lat/lon using lcc_to_latlon(), and collects the extra fields that are
    meaningful for each element type.

    PARAMETERS:
        std_path  --  Path to the .std file.  Returns {} if the file is missing.

    RETURNS:
        A dict keyed by element name (column 0), e.g.:
        {
          "Jim Moore Road B": {
              "etype": "9",          # element type code
              "lat":   34.034,       # WGS84 latitude
              "lon":   -83.907,      # WGS84 longitude
              "phase": "7",          # phase config
              "feeder_name": "70B",  # col 4 (map number used as feeder label for sources)
              "kv":    "230.0",      # col 14 for sources
          },
          "over_21435": {
              "etype":    "10",
              "lat":      34.034,
              "lon":      -83.907,
              "phase":    "7",
              "closed_a": "1",       # is closed phase A (1=closed, 0=open)
              "closed_b": "1",
              "closed_c": "1",
              "feeder":   "Jim Moore Road - 06",  # col 19
              "desc_a":   "ABB",     # description phase A
          },
          "sub_GR139-A": {
              "etype":   "6",
              "lat":     34.034,
              "lon":     -83.907,
              "status":  "C",        # C=closed, O=open
              "sw_id":   "4646",
              "partner": "sub_GR139-B",
          },
          "MP70B": {
              "etype": "5",
              "lat":   34.034,
              "lon":   -83.907,
              "kva":   "50000",
          },
          ...
        }

    COORD SOURCE:
        Column 5 (STD_XL) and column 6 (STD_YL) are the downline / load-side
        endpoint in Georgia West State Plane (EPSG:2240) US Survey Feet.
        These are the device's actual installed location in the field.

    MATCH STRATEGY (used in aggregate_data):
        Substations : outage["Sub"]          -> elements where etype == "9"
        Prot. devices: outage["Next Pro Dvc"] -> elements where etype in ("10","6")
        Transformers : outage["Trans"]        -> elements where etype == "5"
        All names are compared as stripped strings; case is preserved as-is in
        the STD file so mismatches will fall back to approximation silently.

    DEBUG:
        Print sorted(elements.keys()) to see all names in the STD.
        If a substation dot still appears in the wrong place after loading the STD,
        check that the outage "Sub" value exactly matches the STD element name
        (including spaces, hyphens, and capitalisation).
    """
    if not std_path or not os.path.exists(std_path):
        print(f"  STD file not found ({std_path}) -- coordinate fallback will be used.")
        return {}

    print(f"  Loading STD file: {std_path}")
    elements = {}
    counts   = {t: 0 for t in ('9', '10', '6', '5', '1', '3', '8', 'other')}

    with open(std_path, 'r') as fh:
        reader = csv.reader(fh)
        for row in reader:
            if len(row) < 7:
                continue
            name  = row[STD_NAME].strip()
            etype = row[STD_ETYPE].strip()
            phase = row[STD_PHASE].strip()

            # Parse coordinates -- skip rows with missing or non-numeric coords
            try:
                xl = float(row[STD_XL])
                yl = float(row[STD_YL])
            except (ValueError, IndexError):
                continue

            lat, lon = lcc_to_latlon(xl, yl)

            # Build element record with type-specific extra fields
            rec = {'etype': etype, 'lat': lat, 'lon': lon, 'phase': phase}

            if etype == STD_TYPE_SOURCE:          # type 9 — substation
                rec['feeder_name'] = row[STD_MAP].strip()   if len(row) > STD_MAP    else ''
                rec['kv']          = row[14].strip()         if len(row) > 14         else ''
                rec['connection']  = row[16].strip()         if len(row) > 16         else ''
                counts['9'] += 1

            elif etype == STD_TYPE_OCDEV:         # type 10 — recloser/fuse/sectionalizer
                rec['desc_a']   = row[8].strip()             if len(row) > 8          else ''
                rec['desc_b']   = row[9].strip()             if len(row) > 9          else ''
                rec['desc_c']   = row[10].strip()            if len(row) > 10         else ''
                rec['closed_a'] = row[STD_OSTATUSA].strip()  if len(row) > STD_OSTATUSA else ''
                rec['closed_b'] = row[STD_OSTATUSB].strip()  if len(row) > STD_OSTATUSB else ''
                rec['closed_c'] = row[STD_OSTATUSC].strip()  if len(row) > STD_OSTATUSC else ''
                rec['feeder']   = row[19].strip()            if len(row) > 19         else ''
                counts['10'] += 1

            elif etype == STD_TYPE_SWITCH:        # type 6 — switch
                rec['status']   = row[STD_SWSTATUS].strip()  if len(row) > STD_SWSTATUS  else ''
                rec['sw_id']    = row[STD_SWID].strip()      if len(row) > STD_SWID      else ''
                rec['partner']  = row[STD_SWPARTNER].strip() if len(row) > STD_SWPARTNER else ''
                counts['6'] += 1

            elif etype == STD_TYPE_TRANS:         # type 5 — transformer
                rec['kva']      = row[9].strip()             if len(row) > 9          else ''
                rec['kv_in']    = row[10].strip()            if len(row) > 10         else ''
                rec['kv_out']   = row[13].strip()            if len(row) > 13         else ''
                counts['5'] += 1

            elif etype == STD_TYPE_OHSPAN:        # type 1 — overhead span
                counts['1'] += 1

            elif etype == STD_TYPE_UGSPAN:        # type 3 — underground span
                counts['3'] += 1

            elif etype == STD_TYPE_NODE:          # type 8 — node
                counts['8'] += 1

            else:
                counts['other'] += 1

            elements[name] = rec

    print(f"  STD loaded: {len(elements):,} elements")
    print(f"    Sources (substations):      {counts['9']:4d}")
    print(f"    Overcurrent devices:        {counts['10']:4d}")
    print(f"    Switches:                   {counts['6']:4d}")
    print(f"    Transformers:               {counts['5']:4d}")
    print(f"    OH Spans:                   {counts['1']:4d}")
    print(f"    UG Spans:                   {counts['3']:4d}")
    print(f"    Nodes:                      {counts['8']:4d}")
    return elements


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 3 — MPT NETWORK LOADER
# ════════════════════════════════════════════════════════════════════════════════

def _rdp_simplify(points: list, epsilon: float = 0.00005) -> list:
    """
    Douglas-Peucker polyline simplification.

    Recursively removes intermediate vertices that are within `epsilon` degrees
    of the straight line connecting the endpoints.  epsilon=0.00005 deg is
    approximately 5.5 metres -- enough to remove the "noise" vertices that
    WindMil inserts without noticeably changing the line shape at map zoom 9-17.

    PARAMETERS:
        points   -- list of (lat, lon) tuples
        epsilon  -- tolerance in degrees (default 0.00005 deg ~ 5.5 m)

    RETURNS:
        Simplified list of (lat, lon) tuples.
    """
    if len(points) <= 2:
        return points

    # Find the point farthest from the line between the first and last points
    d_max = 0.0
    idx   = 0
    x1, y1 = points[0]
    x2, y2 = points[-1]
    line_len = math.hypot(y2 - y1, x2 - x1)   # length of the chord

    for i in range(1, len(points) - 1):
        x0, y0 = points[i]
        if line_len == 0:
            d = math.hypot(x0 - x1, y0 - y1)
        else:
            # Perpendicular distance from (x0,y0) to line (x1,y1)-(x2,y2)
            d = abs((y2 - y1)*x0 - (x2 - x1)*y0 + x2*y1 - y2*x1) / line_len
        if d > d_max:
            d_max = d
            idx   = i

    if d_max > epsilon:
        left  = _rdp_simplify(points[:idx + 1], epsilon)
        right = _rdp_simplify(points[idx:],     epsilon)
        return left[:-1] + right
    else:
        return [points[0], points[-1]]


def load_mpt_network(mpt_path: str, max_spans: int = None) -> list:
    """
    Parse a Milsoft WindMil MPT file, convert all span polylines from
    Georgia West State Plane (US Survey Feet) to WGS84 lat/lon, and return
    them in a compact format ready for JavaScript injection.

    The MPT file has one row per polyline vertex:
        span_123456, 2375034.43, 1467798.73

    Multiple consecutive rows with the same span name form one polyline.
    The resulting polylines are simplified with Douglas-Peucker to reduce
    the number of vertices before embedding in the HTML file.

    PARAMETERS:
        mpt_path   -- path to the .mpt file
        max_spans  -- if given, only the first max_spans spans are included.
                      Spans are ordered as they appear in the file (which is
                      roughly geographic -- nearby spans tend to cluster).
                      Set to None to include all ~149,000 spans (produces ~8 MB
                      of embedded JS data).

    RETURNS:
        A list of polylines.  Each polyline is a list of [lat, lon] pairs.
        Example: [[[34.034, -83.907], [34.035, -83.906]], ...]

    DEBUG:
        If the returned list is empty, check that mpt_path points to the right file.
        If all lat values are outside 33-35 or all lon values outside -85 to -82,
        the coordinate system may be wrong -- verify the file is GA West SPCS
        (EPSG:2240) and not GA East (EPSG:2239) or a metric projection.
    """
    if not mpt_path or not os.path.exists(mpt_path):
        print(f"  MPT file not found ({mpt_path}) -- network layer will be skipped.")
        return []

    print(f"  Loading MPT network file: {mpt_path}")
    spans        = {}   # span_name -> list of (lat, lon) tuples
    span_order   = []   # preserve insertion order for max_spans slicing
    last_name    = None
    current_pts  = []

    with open(mpt_path, 'r') as fh:
        reader = csv.reader(fh)
        for row in reader:
            if len(row) < 3:
                continue
            name = row[0].strip()
            try:
                x, y = float(row[1]), float(row[2])
            except ValueError:
                continue   # Skip header rows or malformed lines

            lat, lon = lcc_to_latlon(x, y)

            if name != last_name:
                # Save completed span
                if last_name is not None and current_pts:
                    spans[last_name]   = current_pts
                    span_order.append(last_name)
                current_pts = [(lat, lon)]
                last_name   = name
            else:
                current_pts.append((lat, lon))

        # Save the final span
        if last_name is not None and current_pts:
            spans[last_name] = current_pts
            span_order.append(last_name)

    total_spans = len(span_order)
    print(f"  Loaded {total_spans:,} spans from MPT file")

    # Apply max_spans limit
    if max_spans is not None and max_spans < total_spans:
        span_order = span_order[:max_spans]
        print(f"  Limiting to first {max_spans:,} spans (set MAX_SPANS=None for all)")

    # Simplify polylines to reduce embedded data size
    polylines = []
    orig_pts  = 0
    simp_pts  = 0
    for name in span_order:
        pts = spans[name]
        orig_pts += len(pts)
        simplified = _rdp_simplify(pts, epsilon=0.00005)  # ~5.5 m tolerance
        simp_pts  += len(simplified)
        # Convert tuples to lists for JSON serialisation
        polylines.append([[round(p[0], 5), round(p[1], 5)] for p in simplified])

    reduction = 100 * (1 - simp_pts / orig_pts) if orig_pts else 0
    print(f"  Simplified: {orig_pts:,} vertices -> {simp_pts:,} ({reduction:.0f}% reduction)")
    print(f"  Embedded spans: {len(polylines):,}")

    return polylines


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 4 — COORDINATE HELPERS
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
# SECTION 5 — DATA LOADING & VALIDATION
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
# SECTION 6 — DATA AGGREGATION
# ════════════════════════════════════════════════════════════════════════════════

def aggregate_data(outage: pd.DataFrame, clicks: pd.DataFrame,
                   std_elements: dict = None):
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
          - next_pro_dvc          : the protective device name for this substation
          - coord_source          : 'STD' if coordinates came from GIS, 'approx' if hashed

    CLICK AGGREGATION (per RepId):
        Groups rows by RepId and computes:
          - total_clicks / event_count / avg / max / first_seen / last_seen

    COORDINATE PRIORITY:
        1. STD file (real surveyed coordinates) -- used when std_elements is
           provided and the element name matches.
           Substations: outage["Sub"] matched against STD type-9 SOURCE elements.
           Protective devices: outage["Next Pro Dvc"] matched against type-10/6.
        2. MD5-hash fallback -- used for any element not found in the STD.
           Substations fall back to district_offset() (district centre + small hash nudge).
           Reclosers (RepId) always fall back -- RepId is not in the STD.

    PARAMETERS:
        outage       -- clean outage DataFrame from load_data()
        clicks       -- clean click-count DataFrame from load_data()
        std_elements -- dict from load_STD_elements(), or None to skip STD lookup

    RETURNS:
        sub_agg -- per-substation summary DataFrame with lat/lon and coord_source
        rep_agg -- per-recloser summary DataFrame with lat/lon columns
    """
    std_elements = std_elements or {}

    # ── A) PER-SUBSTATION OUTAGE SUMMARY ────────────────────────────────────

    sub_agg = (
        outage.groupby(["Sub", "District"], dropna=True)
        .agg(
            outage_count=("Outage", "count"),
            total_customer_minutes=("Customer Minutes", "sum"),
            avg_duration_hrs=("Duration", lambda x: x.apply(
                lambda d: d.total_seconds() / 3600 if hasattr(d, "total_seconds") else 0
            ).mean()),
            customers_affected=("# Out", "sum"),
            causes=("Cause Desc", lambda x: x.value_counts().head(3).to_dict()),
            # Capture the most common Next Pro Dvc for this substation.
            # This gives us a second lookup path when the Sub name itself
            # doesn't match a STD SOURCE element (e.g. the feeder head
            # device is an OCDEV parented directly to the substation).
            next_pro_dvc=("Next Pro Dvc", lambda x: x.dropna().mode().iloc[0] if len(x.dropna()) else ""),
        )
        .reset_index()
    )

    # ── Assign coordinates to each substation ───────────────────────────────
    # Priority: STD type-9 SOURCE by Sub name
    #        -> STD type-10/6 by Next Pro Dvc name (fallback within STD)
    #        -> MD5 district-hash approximation (last resort)
    std_hit  = 0
    npd_hit  = 0
    approx   = 0

    lats, lons, coord_sources = [], [], []
    for _, row in sub_agg.iterrows():
        sub_name = str(row["Sub"])
        npd_name = str(row["next_pro_dvc"])

        # First try: Sub name -> STD SOURCE (type 9)
        el = std_elements.get(sub_name)
        if el and el['etype'] == STD_TYPE_SOURCE:
            lats.append(el['lat']); lons.append(el['lon'])
            coord_sources.append('STD-source')
            std_hit += 1
            continue

        # Second try: Next Pro Dvc name -> STD OCDEV or SWITCH
        el = std_elements.get(npd_name)
        if el and el['etype'] in (STD_TYPE_OCDEV, STD_TYPE_SWITCH):
            lats.append(el['lat']); lons.append(el['lon'])
            coord_sources.append('STD-device')
            npd_hit += 1
            continue

        # Fallback: MD5-hash district offset
        lat, lon = district_offset(sub_name, str(row["District"]))
        lats.append(lat); lons.append(lon)
        coord_sources.append('approx')
        approx += 1

    sub_agg["lat"]          = lats
    sub_agg["lon"]          = lons
    sub_agg["coord_source"] = coord_sources

    print(f"  Substations summarised: {len(sub_agg)}")
    print(f"    Real coords from STD (source element): {std_hit}")
    print(f"    Real coords from STD (device match):   {npd_hit}")
    print(f"    Approximated (MD5 hash fallback):      {approx}")

    # ── B) PER-RECLOSER CLICK SUMMARY ───────────────────────────────────────

    rep_agg = (
        clicks.groupby("RepId")
        .agg(
            total_clicks=("ClickCountChange", "sum"),
            event_count=("ClickCountChange", "count"),
            avg_clicks_per_window=("ClickCountChange", "mean"),
            max_window_clicks=("ClickCountChange", "max"),
            first_seen=("ChangeWindowStart", "min"),
            last_seen=("ChangeWindowStart", "max"),
        )
        .reset_index()
    )

    cmin = rep_agg["total_clicks"].min()
    cmax = rep_agg["total_clicks"].max()
    rep_agg["click_score"] = (rep_agg["total_clicks"] - cmin) / max(cmax - cmin, 1)

    # ── Assign coordinates to reclosers ─────────────────────────────────────
    # RepId is a SCADA numeric ID with no direct match in the STD file.
    # MD5-hash fallback is used for all reclosers until a RepId->device
    # cross-reference table is available.
    lat_min, lat_max = 33.7, 34.6
    lon_min, lon_max = -84.3, -83.3
    rep_agg = rep_agg.reset_index(drop=True)

    def rep_coords(rep_id):
        h   = int(hashlib.md5(str(rep_id).encode()).hexdigest(), 16)
        lat = lat_min + (h % 10000) / 10000 * (lat_max - lat_min)
        lon = lon_min + ((h >> 16) % 10000) / 10000 * (lon_max - lon_min)
        return lat, lon

    coords = rep_agg["RepId"].apply(
        lambda rid: pd.Series(rep_coords(rid), index=["lat", "lon"])
    )
    rep_agg["lat"] = coords["lat"]
    rep_agg["lon"] = coords["lon"]

    print(f"  Reclosers summarised: {len(rep_agg)} (coords approximated -- no RepId->STD map yet)")

    return sub_agg, rep_agg


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 7 — COLOUR UTILITY
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
               flagged_outages: pd.DataFrame, flagged_clicks: pd.DataFrame,
               network_polylines: list = None) -> str:
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
            "causes":           r["causes"],
            # coord_source tells the popup whether this location is real GIS data
            # ('STD-source', 'STD-device') or an approximation ('approx').
            "coordSource":      str(r.get("coord_source", "approx")),
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

    # Serialize the network polylines (from MPT file).
    # Each polyline is a list of [lat, lon] pairs.
    # If no MPT data was provided, embed an empty array -- the network layer
    # toggle will still appear in the UI but will add nothing to the map.
    network_json = json.dumps(network_polylines or [])
    has_network  = bool(network_polylines)

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

/* ── MAP CONTAINER ───────────────────────────────────────────────────────────
   Leaflet requires the map div to have an explicit, non-zero height.
   flex:1 fills all remaining width after the sidebar. */
#map {{
  flex: 1;
  height: 100%;
  min-height: 0;
  position: relative;
}}

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
        <div class="toggle-row">
          <!-- Network Lines: draws actual wire routes from the WindMil MPT export.
               Checked by default when MPT data is present; hidden when no MPT loaded. -->
          <label for="tog-network" id="tog-network-label"
            style="{'' if has_network else 'opacity:0.35;pointer-events:none'}">
            Network Lines
            {'<span style="font-size:9px;color:var(--accent);margin-left:4px">GIS</span>' if has_network else
             '<span style="font-size:9px;color:var(--muted);margin-left:4px">no MPT</span>'}
          </label>
          <label class="toggle" style="{'' if has_network else 'opacity:0.35;pointer-events:none'}">
            <input type="checkbox" id="tog-network" {'checked' if has_network else ''}
              onchange="toggleLayer('network', this.checked)"/>
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

// NETWORK_LINES — distribution wire polylines from the Milsoft WindMil MPT export.
// Each element is an array of [lat, lon] pairs describing one span route.
// Loaded from:  {MPT_PATH or 'No MPT file configured'}
// Spans embedded: {len(network_polylines) if network_polylines else 0:,}
// Generated by lcc_to_latlon() converting Georgia West State Plane (EPSG:2240)
// coordinates to WGS84 geographic coordinates.
//
// To inspect: console.log('Network spans loaded:', NETWORK_LINES.length)
const NETWORK_LINES = {network_json};

// ════════════════════════════════════════════════════════════════════════════
// MAP INITIALISATION
// ════════════════════════════════════════════════════════════════════════════
// scrollWheelZoom: true  → lets users zoom with the mouse scroll wheel
// dragging: true         → lets users click-and-drag to pan (on by default,
//                          but listed explicitly for clarity)
// doubleClickZoom: true  → double-click zooms in
const map = L.map('map', {{
  center: [33.95, -83.85],   // NE Georgia centre point
  zoom: 9,                    // Zoom 9 ≈ county level — shows the whole service area
  zoomControl: true,          // Shows the +/- zoom buttons in the top-left corner
  scrollWheelZoom: true,      // Scroll wheel zooms in/out
  dragging: true,             // Click and drag to pan
  doubleClickZoom: true,      // Double-click to zoom in
  touchZoom: true,            // Pinch-to-zoom on touchscreens
}});

// ── SATELLITE TILE LAYER ──────────────────────────────────────────────────────
// Uses ESRI World Imagery — the same satellite photo dataset behind many mapping
// apps.  Free to use, no API key required, loads directly in the browser.
// Zoom levels 1–19 are supported; at zoom 17+ individual buildings are visible.
//
// HOW TILE URLS WORK:
//   {{z}} = zoom level (integer)
//   {{x}} = tile column number (increases left to right)
//   {{y}} = tile row number (increases top to bottom)
// Leaflet automatically fills these in for every visible tile.
L.tileLayer(
  'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
    attribution: 'Tiles © Esri — Source: Esri, Maxar, Earthstar Geographics',
    maxZoom: 19,
  }}
).addTo(map);

// ── ROAD / LABEL OVERLAY ──────────────────────────────────────────────────────
// ESRI satellite tiles don't include road names or city labels.
// This semi-transparent overlay adds roads, city names, and county boundaries
// on top of the satellite imagery so you can orient yourself on the map.
L.tileLayer(
  'https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
    attribution: '',
    maxZoom: 19,
    opacity: 0.6,   // Reduce opacity so satellite imagery still shows through
  }}
).addTo(map);

// ════════════════════════════════════════════════════════════════════════════
// LAYER GROUPS
// ════════════════════════════════════════════════════════════════════════════
// Leaflet LayerGroups bundle multiple markers together so we can show/hide
// an entire category of markers with a single map.addLayer() / removeLayer() call.
// heatmap and outageheat start as null — they're built lazily when first toggled.
// network starts as null — built by buildNetworkLayer() on page load if MPT data exists.
const layers = {{
  reclosers:  L.layerGroup().addTo(map),  // All recloser circle markers
  substations:L.layerGroup().addTo(map),  // All substation circle markers
  heatmap:    null,   // Click-count heatmap (built by buildClickHeatmap)
  outageheat: null,   // Outage severity heatmap (built by buildOutageHeatmap)
  network:    null,   // Distribution wire polylines (built by buildNetworkLayer)
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
// NETWORK LINE RENDERING  (from MPT / WindMil GIS export)
// ════════════════════════════════════════════════════════════════════════════

/**
 * buildNetworkLayer()
 * Draw the real distribution network wire routes on the map using the polyline
 * data converted from the Milsoft WindMil MPT GIS export.
 *
 * Each element of NETWORK_LINES is an array of [lat, lon] pairs describing
 * one span's routed path through the field.  Spans are drawn as thin semi-
 * transparent polylines so they provide geographic context without obscuring
 * the recloser dots or substation rings drawn on top.
 *
 * This function is called once on page load (if NETWORK_LINES has data) and
 * also when the user toggles the "Network Lines" layer switch back on.
 *
 * Rendering strategy:
 *   - Zoom < 12  → weight 1px, opacity 0.45  (fine mesh, doesn't clutter)
 *   - Zoom >= 12 → weight 1.5px, opacity 0.65 (thicker lines when zoomed in)
 *   The map.on('zoomend') listener updates line weight automatically.
 *
 * DEBUG:
 *   If lines don't appear, check:
 *     console.log('Network lines:', NETWORK_LINES.length)
 *   If lines appear in the wrong place, the coordinate conversion may have
 *   used the wrong State Plane zone -- verify lcc_to_latlon() parameters.
 */
function buildNetworkLayer() {{
  if (!NETWORK_LINES || NETWORK_LINES.length === 0) return;

  // Remove existing network layer if present (e.g. after a toggle-off/on cycle)
  if (layers.network) map.removeLayer(layers.network);

  // Determine initial style based on current zoom
  const z = map.getZoom();
  const initialWeight  = z >= 12 ? 1.5 : 1.0;
  const initialOpacity = z >= 12 ? 0.65 : 0.45;

  // Build a GeoJSON FeatureCollection from the polyline arrays.
  // Using Leaflet's L.geoJSON() lets us update the style globally later.
  const features = NETWORK_LINES.map(pts => ({{
    type: 'Feature',
    geometry: {{
      type: 'LineString',
      // GeoJSON uses [lon, lat] order (opposite of Leaflet's [lat, lon])
      coordinates: pts.map(p => [p[1], p[0]])
    }},
    properties: {{}}
  }}));

  layers.network = L.geoJSON(
    {{ type: 'FeatureCollection', features }},
    {{
      style: {{
        color:   '#00c8ff',   // Cyan -- matches the dashboard accent colour
        weight:  initialWeight,
        opacity: initialOpacity,
        // No fill for LineString features; this line is ignored but harmless
        fillOpacity: 0
      }},
      // Disable mouse interaction on individual line segments to avoid
      // triggering hover events that slow rendering on 50k+ polylines
      interactive: false
    }}
  ).addTo(map);

  // Adjust line weight dynamically when user zooms in/out
  map.on('zoomend', function() {{
    if (!layers.network) return;
    const z = map.getZoom();
    layers.network.setStyle({{
      weight:  z >= 12 ? 1.5 : 1.0,
      opacity: z >= 12 ? 0.65 : 0.45
    }});
  }});
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
      <span class="popup-key">Location</span>
      <span class="popup-val" style="font-size:10px;color:${{
        s.coordSource === 'STD-source' ? '#00ff99' :
        s.coordSource === 'STD-device' ? '#00c8ff' : '#ffaa00'
      }}">
        ${{s.coordSource === 'STD-source' ? '✓ GIS (substation record)' :
           s.coordSource === 'STD-device' ? '✓ GIS (device record)'    :
                                            '⚠ Approximate (no GIS match)'}}
      </span>
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
  }} else if (name === 'network') {{
    // Network layer: build it on first show (it may not exist yet)
    if (show) {{
      if (!layers.network) {{
        buildNetworkLayer();   // Convert NETWORK_LINES -> Leaflet GeoJSON layer
      }} else {{
        layers.network.addTo(map);
      }}
    }} else {{
      if (layers.network) map.removeLayer(layers.network);
    }}
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
// The order matters: network lines should be drawn BEFORE dots so they sit
// below the recloser and substation markers in the rendering stack.
// ════════════════════════════════════════════════════════════════════════════

buildNetworkLayer();      // Step 1: Draw real wire routes from GIS (network base)
buildRecloserMarkers();   // Step 2: Draw recloser dots (data layer, on top of wires)
buildSubstationMarkers(); // Step 3: Draw substation rings (data layer)
buildClickHeatmap();      // Step 4: Pre-build click heatmap (starts hidden)
buildOutageHeatmap();     // Step 5: Pre-build outage heatmap (starts hidden)
buildRecloserList();      // Step 6: Populate Reclosers sidebar tab
buildOutageList();        // Step 7: Populate Outages sidebar tab
buildFlaggedList();       // Step 8: Populate Flagged sidebar tab

</script>
</body>
</html>"""
    return html


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 9 — ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

def main():
    """
    Orchestrate the full pipeline:
        load_STD_elements → load_mpt_network → load_data → aggregate_data → build_html

    Accepts optional command-line arguments:
        argv[1] – path to the input .xlsx file     (default: EXCEL_PATH)
        argv[2] – path for the output .html file   (default: OUTPUT_HTML)
        argv[3] – path to the WindMil .mpt file    (default: MPT_PATH)
        argv[4] – path to the WindMil .std file    (default: STD_PATH)

    Example:
        python3 generate_map.py                                  # all defaults
        python3 generate_map.py my_data.xlsx                     # custom input
        python3 generate_map.py my_data.xlsx report.html         # custom in+out
        python3 generate_map.py d.xlsx out.html net.mpt net.std  # full GIS data
    """
    path     = sys.argv[1] if len(sys.argv) > 1 else EXCEL_PATH
    output   = sys.argv[2] if len(sys.argv) > 2 else OUTPUT_HTML
    mpt_path = sys.argv[3] if len(sys.argv) > 3 else MPT_PATH
    std_path = sys.argv[4] if len(sys.argv) > 4 else STD_PATH

    print(f"\n{'='*55}")
    print(" Georgia Outage & Click Count Map Generator")
    print(f"{'='*55}")
    print(f"Input:    {path}")
    print(f"Output:   {output}")
    print(f"MPT file: {mpt_path or '(none)'}")
    print(f"STD file: {std_path or '(none)'}\n")

    # ── STEP 1: Load STD element coordinates (real GIS locations) ───────────
    # Done first — fast (~0.1 s) and needed by aggregate_data for coordinate lookup.
    # Returns {} if file is missing; aggregate_data falls back to MD5-hash in that case.
    std_elements = load_STD_elements(std_path)

    # ── STEP 2: Load distribution network polylines from the MPT file ────────
    # Slower step (~30 s for full 149k-span file). Returns [] if file missing.
    network_polylines = load_mpt_network(mpt_path, max_spans=MAX_SPANS)

    # ── STEP 3: Load and validate all data from the workbook ─────────────────
    outage, flagged_outages, clicks, flagged_clicks = load_data(path)

    # ── STEP 4: Aggregate — uses STD coords wherever names match ─────────────
    sub_agg, rep_agg = aggregate_data(outage, clicks, std_elements=std_elements)

    # ── STEP 5: Build the complete HTML string ────────────────────────────────
    html = build_html(sub_agg, rep_agg, flagged_outages, flagged_clicks,
                      network_polylines=network_polylines)

    # ── STEP 6: Write the HTML to disk ───────────────────────────────────────
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(output) // 1024
    print(f"\n✓ Map written to: {output} ({size_kb} KB)")
    print(f"  Open in any browser to explore the interactive map.\n")


# Standard Python idiom: only run main() when this file is executed directly,
# not when it's imported as a module by another script.
if __name__ == "__main__":
    main()