#!/usr/bin/env python3
"""
================================================================================
Outage & Click Count Map Generator  —  FULLY OFFLINE VERSION
================================================================================
PURPOSE:
    Reads Click_Count_and_Outage_Management_Data.xlsx and produces a single
    completely self-contained HTML file.  No internet connection is required
    at any point — not during generation, not when opening the HTML in a browser.

APPROACH:
    Instead of relying on online tile services (CartoDB, OpenStreetMap, etc.),
    this script renders Georgia's 159 counties as SVG <polygon> elements directly
    from embedded coordinate data.  That SVG is injected into an HTML page
    alongside all the processed Excel data as JSON, and vanilla JavaScript
    handles all interactivity (hover tooltips, click popups, filtering,
    heatmap overlay, sidebar panels) without any external dependencies.

    Offline stack:
        Embedded county coords  →  SVG polygon strings
        Python json             →  JS constants for all data
        Vanilla JS + Canvas API →  dots, heatmap, tooltips, popups, filters

USAGE:
    python3 generate_map.py                              # uses defaults below
    python3 generate_map.py my_data.xlsx                 # custom input file
    python3 generate_map.py my_data.xlsx my_output.html  # custom in + out

DEPENDENCIES:
    pip install pandas openpyxl
    (hashlib, json, sys, os, datetime are all Python standard library)

STRUCTURE:
    1.  Imports & Config
    2.  Georgia County Data      – simplified boundary polygons (embedded)
    3.  Coordinate helpers       – geo_to_svg(), district_offset()
    4.  load_data()              – reads, validates, flags bad rows
    5.  aggregate_data()         – per-substation and per-recloser summaries
    6.  click_color()            – 0–1 score to green/yellow/red hex
    7.  build_svg_map()          – builds SVG string from county coordinates
    8.  build_html()             – assembles final HTML/CSS/JS document
    9.  main()                   – orchestrates the full pipeline
================================================================================
"""

import json
import sys
import os
import hashlib
from datetime import datetime

import pandas as pd

# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

EXCEL_PATH  = "Click_Count_and_Outage_Management_Data.xlsx"
OUTPUT_HTML = "outage_map.html"

# Geographic bounding box for the rendered map (degrees).
# Covers all of Georgia with a small margin on each side.
# Adjust these if the map looks clipped or if you zoom to a sub-region.
MAP_LAT_MIN, MAP_LAT_MAX = 30.20, 35.10
MAP_LON_MIN, MAP_LON_MAX = -85.75, -80.75

# NE Georgia district centre points — used to anchor substation coordinates.
# All four districts are in NE Georgia. If districts change, add them here.
DISTRICT_COORDS = {
    "Gainesville":   (34.2979, -83.8241),
    "Jefferson":     (34.1123, -83.5999),
    "Lawrenceville": (33.9566, -83.9880),
    "Neese":         (34.1500, -83.9000),
}

# SVG canvas dimensions in pixels.
# These define the internal coordinate system of the map SVG.
# The SVG scales to fill the browser window, so these only affect
# relative precision — larger values = more precise positioning.
SVG_WIDTH  = 700
SVG_HEIGHT = 820


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2 — GEORGIA COUNTY BOUNDARY DATA
# ════════════════════════════════════════════════════════════════════════════════
# Simplified outlines of Georgia's 159 counties, stored as (lon, lat) tuples.
# Each county is approximated as a bounding-box rectangle — precise enough
# for a service-area overview map, without requiring a shapefile library.
#
# To improve precision: replace the 4-point boxes with actual polygon vertices
# from a shapefile (e.g. using geopandas + Census TIGER data), keeping the
# same dict structure: county_name → list of (lon, lat) tuples.
#
# Counties in NE_GEORGIA_COUNTIES are rendered with a highlight colour to
# show the service territory at a glance.

NE_GEORGIA_COUNTIES = {
    "Hall","Banks","Jackson","Barrow","Gwinnett","Forsyth",
    "Lumpkin","White","Habersham","Stephens","Franklin",
    "Madison","Oglethorpe","Clarke","Oconee"
}

# fmt: off
GEORGIA_COUNTIES = {
    "Appling":      [(-82.61,31.27),(-82.05,31.27),(-82.05,31.71),(-82.61,31.71)],
    "Atkinson":     [(-83.00,31.04),(-82.55,31.04),(-82.55,31.37),(-83.00,31.37)],
    "Bacon":        [(-82.60,31.37),(-82.32,31.37),(-82.32,31.62),(-82.60,31.62)],
    "Baker":        [(-84.59,31.27),(-84.25,31.27),(-84.25,31.62),(-84.59,31.62)],
    "Baldwin":      [(-83.40,33.07),(-83.11,33.07),(-83.11,33.35),(-83.40,33.35)],
    "Banks":        [(-83.72,34.23),(-83.45,34.23),(-83.45,34.50),(-83.72,34.50)],
    "Barrow":       [(-83.89,33.97),(-83.63,33.97),(-83.63,34.17),(-83.89,34.17)],
    "Bartow":       [(-84.97,34.07),(-84.58,34.07),(-84.58,34.42),(-84.97,34.42)],
    "Ben Hill":     [(-83.27,31.65),(-82.98,31.65),(-82.98,31.95),(-83.27,31.95)],
    "Berrien":      [(-83.48,31.08),(-83.16,31.08),(-83.16,31.37),(-83.48,31.37)],
    "Bibb":         [(-83.84,32.75),(-83.57,32.75),(-83.57,33.00),(-83.84,33.00)],
    "Bleckley":     [(-83.47,32.52),(-83.25,32.52),(-83.25,32.74),(-83.47,32.74)],
    "Brantley":     [(-82.25,31.17),(-81.88,31.17),(-81.88,31.62),(-82.25,31.62)],
    "Brooks":       [(-83.65,30.73),(-83.35,30.73),(-83.35,31.08),(-83.65,31.08)],
    "Bryan":        [(-81.65,31.70),(-81.30,31.70),(-81.30,32.13),(-81.65,32.13)],
    "Bulloch":      [(-82.00,32.20),(-81.60,32.20),(-81.60,32.65),(-82.00,32.65)],
    "Burke":        [(-81.95,32.88),(-81.55,32.88),(-81.55,33.27),(-81.95,33.27)],
    "Butts":        [(-84.09,33.14),(-83.81,33.14),(-83.81,33.40),(-84.09,33.40)],
    "Calhoun":      [(-84.81,31.46),(-84.57,31.46),(-84.57,31.77),(-84.81,31.77)],
    "Camden":       [(-82.10,30.60),(-81.73,30.60),(-81.73,31.17),(-82.10,31.17)],
    "Candler":      [(-82.27,32.37),(-82.05,32.37),(-82.05,32.62),(-82.27,32.62)],
    "Carroll":      [(-85.18,33.55),(-84.85,33.55),(-84.85,33.88),(-85.18,33.88)],
    "Catoosa":      [(-85.36,34.69),(-85.04,34.69),(-85.04,34.99),(-85.36,34.99)],
    "Charlton":     [(-82.47,30.60),(-81.98,30.60),(-81.98,31.07),(-82.47,31.07)],
    "Chatham":      [(-81.35,31.87),(-80.84,31.87),(-80.84,32.22),(-81.35,32.22)],
    "Chattahoochee":[(-84.93,32.15),(-84.73,32.15),(-84.73,32.42),(-84.93,32.42)],
    "Chattooga":    [(-85.55,34.21),(-85.21,34.21),(-85.21,34.53),(-85.55,34.53)],
    "Cherokee":     [(-84.63,34.19),(-84.24,34.19),(-84.24,34.55),(-84.63,34.55)],
    "Clarke":       [(-83.50,33.87),(-83.30,33.87),(-83.30,34.06),(-83.50,34.06)],
    "Clay":         [(-85.12,31.46),(-84.88,31.46),(-84.88,31.78),(-85.12,31.78)],
    "Clayton":      [(-84.48,33.48),(-84.24,33.48),(-84.24,33.71),(-84.48,33.71)],
    "Clinch":       [(-83.10,30.73),(-82.63,30.73),(-82.63,31.18),(-83.10,31.18)],
    "Cobb":         [(-84.74,33.73),(-84.35,33.73),(-84.35,34.07),(-84.74,34.07)],
    "Coffee":       [(-83.20,31.37),(-82.72,31.37),(-82.72,31.80),(-83.20,31.80)],
    "Colquitt":     [(-83.97,31.08),(-83.60,31.08),(-83.60,31.50),(-83.97,31.50)],
    "Columbia":     [(-82.35,33.48),(-82.07,33.48),(-82.07,33.82),(-82.35,33.82)],
    "Cook":         [(-83.48,31.05),(-83.20,31.05),(-83.20,31.37),(-83.48,31.37)],
    "Coweta":       [(-85.00,33.32),(-84.68,33.32),(-84.68,33.65),(-85.00,33.65)],
    "Crawford":     [(-84.12,32.55),(-83.88,32.55),(-83.88,32.81),(-84.12,32.81)],
    "Crisp":        [(-83.83,31.75),(-83.55,31.75),(-83.55,32.06),(-83.83,32.06)],
    "Dade":         [(-85.61,34.67),(-85.36,34.67),(-85.36,34.99),(-85.61,34.99)],
    "Dawson":       [(-84.23,34.38),(-84.00,34.38),(-84.00,34.63),(-84.23,34.63)],
    "Decatur":      [(-84.60,30.73),(-84.25,30.73),(-84.25,31.10),(-84.60,31.10)],
    "DeKalb":       [(-84.26,33.64),(-84.00,33.64),(-84.00,33.90),(-84.26,33.90)],
    "Dodge":        [(-83.40,31.95),(-82.97,31.95),(-82.97,32.40),(-83.40,32.40)],
    "Dooly":        [(-84.03,32.00),(-83.73,32.00),(-83.73,32.35),(-84.03,32.35)],
    "Dougherty":    [(-84.32,31.47),(-84.01,31.47),(-84.01,31.79),(-84.32,31.79)],
    "Douglas":      [(-84.89,33.60),(-84.68,33.60),(-84.68,33.87),(-84.89,33.87)],
    "Early":        [(-85.10,31.29),(-84.72,31.29),(-84.72,31.66),(-85.10,31.66)],
    "Echols":       [(-83.38,30.60),(-82.97,30.60),(-82.97,31.00),(-83.38,31.00)],
    "Effingham":    [(-81.65,32.15),(-81.24,32.15),(-81.24,32.60),(-81.65,32.60)],
    "Elbert":       [(-83.03,34.02),(-82.68,34.02),(-82.68,34.38),(-83.03,34.38)],
    "Emanuel":      [(-82.55,32.28),(-82.10,32.28),(-82.10,32.72),(-82.55,32.72)],
    "Evans":        [(-82.10,32.17),(-81.83,32.17),(-81.83,32.43),(-82.10,32.43)],
    "Fannin":       [(-84.39,34.63),(-84.07,34.63),(-84.07,34.99),(-84.39,34.99)],
    "Fayette":      [(-84.59,33.30),(-84.32,33.30),(-84.32,33.58),(-84.59,33.58)],
    "Floyd":        [(-85.47,34.07),(-85.07,34.07),(-85.07,34.47),(-85.47,34.47)],
    "Forsyth":      [(-84.23,34.17),(-84.00,34.17),(-84.00,34.43),(-84.23,34.43)],
    "Franklin":     [(-83.31,34.38),(-83.04,34.38),(-83.04,34.65),(-83.31,34.65)],
    "Fulton":       [(-84.64,33.52),(-84.29,33.52),(-84.29,34.12),(-84.64,34.12)],
    "Gilmer":       [(-84.52,34.57),(-84.24,34.57),(-84.24,34.88),(-84.52,34.88)],
    "Glascock":     [(-82.72,33.06),(-82.48,33.06),(-82.48,33.27),(-82.72,33.27)],
    "Glynn":        [(-81.73,31.22),(-81.30,31.22),(-81.30,31.60),(-81.73,31.60)],
    "Gordon":       [(-85.08,34.35),(-84.72,34.35),(-84.72,34.65),(-85.08,34.65)],
    "Grady":        [(-84.31,30.73),(-83.97,30.73),(-83.97,31.10),(-84.31,31.10)],
    "Greene":       [(-83.28,33.52),(-83.01,33.52),(-83.01,33.78),(-83.28,33.78)],
    "Gwinnett":     [(-84.08,33.81),(-83.80,33.81),(-83.80,34.15),(-84.08,34.15)],
    "Habersham":    [(-83.62,34.47),(-83.37,34.47),(-83.37,34.75),(-83.62,34.75)],
    "Hall":         [(-83.93,34.19),(-83.61,34.19),(-83.61,34.54),(-83.93,34.54)],
    "Hancock":      [(-83.12,33.05),(-82.82,33.05),(-82.82,33.37),(-83.12,33.37)],
    "Haralson":     [(-85.39,33.57),(-85.10,33.57),(-85.10,33.87),(-85.39,33.87)],
    "Harris":       [(-85.11,32.62),(-84.78,32.62),(-84.78,32.98),(-85.11,32.98)],
    "Hart":         [(-83.02,34.22),(-82.73,34.22),(-82.73,34.51),(-83.02,34.51)],
    "Heard":        [(-85.30,33.29),(-85.02,33.29),(-85.02,33.59),(-85.30,33.59)],
    "Henry":        [(-84.29,33.39),(-84.01,33.39),(-84.01,33.65),(-84.29,33.65)],
    "Houston":      [(-83.85,32.44),(-83.52,32.44),(-83.52,32.77),(-83.85,32.77)],
    "Irwin":        [(-83.63,31.55),(-83.30,31.55),(-83.30,31.85),(-83.63,31.85)],
    "Jackson":      [(-83.69,34.07),(-83.40,34.07),(-83.40,34.37),(-83.69,34.37)],
    "Jasper":       [(-83.74,33.22),(-83.49,33.22),(-83.49,33.48),(-83.74,33.48)],
    "Jeff Davis":   [(-82.87,31.57),(-82.50,31.57),(-82.50,31.93),(-82.87,31.93)],
    "Jefferson":    [(-82.60,33.05),(-82.24,33.05),(-82.24,33.46),(-82.60,33.46)],
    "Jenkins":      [(-82.02,32.65),(-81.65,32.65),(-81.65,32.97),(-82.02,32.97)],
    "Johnson":      [(-82.95,32.52),(-82.62,32.52),(-82.62,32.83),(-82.95,32.83)],
    "Jones":        [(-83.58,32.99),(-83.29,32.99),(-83.29,33.25),(-83.58,33.25)],
    "Lamar":        [(-84.21,33.00),(-84.00,33.00),(-84.00,33.24),(-84.21,33.24)],
    "Lanier":       [(-83.07,31.02),(-82.77,31.02),(-82.77,31.27),(-83.07,31.27)],
    "Laurens":      [(-83.24,32.25),(-82.73,32.25),(-82.73,32.73),(-83.24,32.73)],
    "Lee":          [(-84.24,31.64),(-83.97,31.64),(-83.97,31.95),(-84.24,31.95)],
    "Liberty":      [(-81.56,31.60),(-81.18,31.60),(-81.18,32.00),(-81.56,32.00)],
    "Lincoln":      [(-82.77,33.73),(-82.53,33.73),(-82.53,33.99),(-82.77,33.99)],
    "Long":         [(-81.87,31.57),(-81.53,31.57),(-81.53,31.93),(-81.87,31.93)],
    "Lowndes":      [(-83.60,30.75),(-83.22,30.75),(-83.22,31.10),(-83.60,31.10)],
    "Lumpkin":      [(-84.07,34.50),(-83.88,34.50),(-83.88,34.78),(-84.07,34.78)],
    "Macon":        [(-84.36,32.35),(-84.05,32.35),(-84.05,32.63),(-84.36,32.63)],
    "Madison":      [(-83.30,33.97),(-83.03,33.97),(-83.03,34.25),(-83.30,34.25)],
    "Marion":       [(-84.67,32.38),(-84.39,32.38),(-84.39,32.65),(-84.67,32.65)],
    "McDuffie":     [(-82.57,33.44),(-82.31,33.44),(-82.31,33.72),(-82.57,33.72)],
    "McIntosh":     [(-81.73,31.33),(-81.32,31.33),(-81.32,31.65),(-81.73,31.65)],
    "Meriwether":   [(-85.07,32.90),(-84.73,32.90),(-84.73,33.22),(-85.07,33.22)],
    "Miller":       [(-84.93,31.13),(-84.64,31.13),(-84.64,31.47),(-84.93,31.47)],
    "Mitchell":     [(-84.42,31.07),(-84.10,31.07),(-84.10,31.47),(-84.42,31.47)],
    "Monroe":       [(-84.20,32.87),(-83.91,32.87),(-83.91,33.13),(-84.20,33.13)],
    "Montgomery":   [(-82.55,31.93),(-82.25,31.93),(-82.25,32.23),(-82.55,32.23)],
    "Morgan":       [(-83.61,33.52),(-83.36,33.52),(-83.36,33.77),(-83.61,33.77)],
    "Murray":       [(-84.84,34.62),(-84.55,34.62),(-84.55,34.93),(-84.84,34.93)],
    "Muscogee":     [(-85.03,32.37),(-84.82,32.37),(-84.82,32.67),(-85.03,32.67)],
    "Newton":       [(-83.99,33.48),(-83.72,33.48),(-83.72,33.76),(-83.99,33.76)],
    "Oconee":       [(-83.52,33.73),(-83.30,33.73),(-83.30,33.97),(-83.52,33.97)],
    "Oglethorpe":   [(-83.18,33.78),(-82.93,33.78),(-82.93,34.07),(-83.18,34.07)],
    "Paulding":     [(-85.02,33.75),(-84.74,33.75),(-84.74,34.02),(-85.02,34.02)],
    "Peach":        [(-84.22,32.45),(-83.99,32.45),(-83.99,32.68),(-84.22,32.68)],
    "Pickens":      [(-84.49,34.42),(-84.23,34.42),(-84.23,34.68),(-84.49,34.68)],
    "Pierce":       [(-82.30,31.37),(-81.92,31.37),(-81.92,31.67),(-82.30,31.67)],
    "Pike":         [(-84.38,32.90),(-84.13,32.90),(-84.13,33.14),(-84.38,33.14)],
    "Polk":         [(-85.34,33.90),(-85.07,33.90),(-85.07,34.18),(-85.34,34.18)],
    "Pulaski":      [(-83.55,31.99),(-83.29,31.99),(-83.29,32.26),(-83.55,32.26)],
    "Putnam":       [(-83.49,33.24),(-83.23,33.24),(-83.23,33.52),(-83.49,33.52)],
    "Quitman":      [(-85.12,31.63),(-84.93,31.63),(-84.93,31.90),(-85.12,31.90)],
    "Rabun":        [(-83.54,34.73),(-83.25,34.73),(-83.25,34.99),(-83.54,34.99)],
    "Randolph":     [(-85.04,31.63),(-84.78,31.63),(-84.78,31.98),(-85.04,31.98)],
    "Richmond":     [(-82.35,33.08),(-81.97,33.08),(-81.97,33.48),(-82.35,33.48)],
    "Rockdale":     [(-84.03,33.57),(-83.80,33.57),(-83.80,33.78),(-84.03,33.78)],
    "Schley":       [(-84.50,32.22),(-84.28,32.22),(-84.28,32.46),(-84.50,32.46)],
    "Screven":      [(-81.87,32.53),(-81.48,32.53),(-81.48,33.00),(-81.87,33.00)],
    "Seminole":     [(-84.93,30.73),(-84.62,30.73),(-84.62,31.07),(-84.93,31.07)],
    "Spalding":     [(-84.41,33.10),(-84.16,33.10),(-84.16,33.35),(-84.41,33.35)],
    "Stephens":     [(-83.32,34.50),(-83.07,34.50),(-83.07,34.75),(-83.32,34.75)],
    "Stewart":      [(-85.09,31.96),(-84.77,31.96),(-84.77,32.30),(-85.09,32.30)],
    "Sumter":       [(-84.32,31.77),(-84.00,31.77),(-84.00,32.12),(-84.32,32.12)],
    "Talbot":       [(-84.74,32.63),(-84.40,32.63),(-84.40,32.92),(-84.74,32.92)],
    "Taliaferro":   [(-82.87,33.40),(-82.62,33.40),(-82.62,33.62),(-82.87,33.62)],
    "Tattnall":     [(-82.38,31.84),(-81.97,31.84),(-81.97,32.27),(-82.38,32.27)],
    "Taylor":       [(-84.55,32.23),(-84.25,32.23),(-84.25,32.55),(-84.55,32.55)],
    "Telfair":      [(-83.22,31.85),(-82.87,31.85),(-82.87,32.22),(-83.22,32.22)],
    "Terrell":      [(-84.56,31.63),(-84.28,31.63),(-84.28,31.93),(-84.56,31.93)],
    "Thomas":       [(-84.20,30.73),(-83.82,30.73),(-83.82,31.10),(-84.20,31.10)],
    "Tift":         [(-83.72,31.48),(-83.43,31.48),(-83.43,31.80),(-83.72,31.80)],
    "Toombs":       [(-82.57,31.93),(-82.22,31.93),(-82.22,32.25),(-82.57,32.25)],
    "Towns":        [(-83.87,34.72),(-83.59,34.72),(-83.59,34.99),(-83.87,34.99)],
    "Treutlen":     [(-82.68,32.37),(-82.41,32.37),(-82.41,32.62),(-82.68,32.62)],
    "Troup":        [(-85.19,32.96),(-84.86,32.96),(-84.86,33.30),(-85.19,33.30)],
    "Turner":       [(-83.62,31.55),(-83.30,31.55),(-83.30,31.88),(-83.62,31.88)],
    "Twiggs":       [(-83.60,32.60),(-83.34,32.60),(-83.34,32.87),(-83.60,32.87)],
    "Union":        [(-84.04,34.73),(-83.82,34.73),(-83.82,34.99),(-84.04,34.99)],
    "Upson":        [(-84.42,32.82),(-84.13,32.82),(-84.13,33.11),(-84.42,33.11)],
    "Walker":       [(-85.47,34.45),(-85.12,34.45),(-85.12,34.78),(-85.47,34.78)],
    "Walton":       [(-83.77,33.67),(-83.51,33.67),(-83.51,33.98),(-83.77,33.98)],
    "Ware":         [(-82.79,31.03),(-82.36,31.03),(-82.36,31.50),(-82.79,31.50)],
    "Warren":       [(-82.72,33.38),(-82.46,33.38),(-82.46,33.65),(-82.72,33.65)],
    "Washington":   [(-82.99,32.85),(-82.62,32.85),(-82.62,33.23),(-82.99,33.23)],
    "Wayne":        [(-82.14,31.55),(-81.78,31.55),(-81.78,31.95),(-82.14,31.95)],
    "Webster":      [(-84.68,31.93),(-84.47,31.93),(-84.47,32.20),(-84.68,32.20)],
    "Wheeler":      [(-82.90,31.97),(-82.55,31.97),(-82.55,32.28),(-82.90,32.28)],
    "White":        [(-83.74,34.47),(-83.53,34.47),(-83.53,34.74),(-83.74,34.74)],
    "Whitfield":    [(-85.07,34.60),(-84.78,34.60),(-84.78,34.92),(-85.07,34.92)],
    "Wilcox":       [(-83.63,31.70),(-83.30,31.70),(-83.30,32.05),(-83.63,32.05)],
    "Wilkes":       [(-82.98,33.78),(-82.71,33.78),(-82.71,34.10),(-82.98,34.10)],
    "Wilkinson":    [(-83.27,32.72),(-82.97,32.72),(-82.97,33.07),(-83.27,33.07)],
    "Worth":        [(-84.00,31.52),(-83.65,31.52),(-83.65,31.87),(-84.00,31.87)],
}
# fmt: on


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 3 — COORDINATE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def district_offset(name: str, district: str) -> tuple:
    """
    Stable (deterministic) lat/lon offset from a district centre for a substation.
    Uses MD5 hash of the substation name so the same substation always lands at
    the same spot across multiple script runs.
    """
    h = int(hashlib.md5(name.encode()).hexdigest(), 16)
    lat_off = ((h % 200) - 100) / 1000.0         # ± ~11 km
    lon_off = (((h >> 8) % 200) - 100) / 1000.0  # ± ~9 km
    base = DISTRICT_COORDS.get(district, (34.0, -83.8))
    return (base[0] + lat_off, base[1] + lon_off)


def geo_to_svg(lat: float, lon: float) -> tuple:
    """
    Convert geographic (lat, lon) to SVG pixel (x, y).
    Uses a simple linear (equirectangular) projection across the map bounding box.
    SVG y increases downward, so latitude is inverted.

    Returns (x, y) as floats in the SVG_WIDTH × SVG_HEIGHT coordinate space.
    """
    x = (lon - MAP_LON_MIN) / (MAP_LON_MAX - MAP_LON_MIN) * SVG_WIDTH
    y = (1.0 - (lat - MAP_LAT_MIN) / (MAP_LAT_MAX - MAP_LAT_MIN)) * SVG_HEIGHT
    return (x, y)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DATA LOADING & VALIDATION
# ════════════════════════════════════════════════════════════════════════════════

def load_data(path: str):
    """
    Open the Excel workbook, load all sheets, flag rows missing critical data,
    and return four DataFrames: (clean_outage, flagged_outage, clean_clicks, flagged_clicks).

    Critical columns — outage:      Outage, Time Off, District, Map Location
    Critical columns — click count: RepId, MeterId, ChangeWindowStart, ClickCountChange

    DEBUG:
        • KeyError on sheet name → workbook was renamed. Print xl.sheet_names.
        • Unexpected flagged count → add print(outage_raw[outage_raw['_flagged']])
          just before return to inspect which rows are affected.
    """
    if not os.path.exists(path):
        print(f"ERROR: File not found: {path}", file=sys.stderr)
        sys.exit(1)

    xl = pd.ExcelFile(path)
    print(f"✓ Loaded workbook: {xl.sheet_names}")

    # ── Outage sheet ──────────────────────────────────────────────────────────
    outage_raw = xl.parse("Outage Data 09-17 to 09-25")

    critical_cols = ["Outage", "Time Off", "District", "Map Location"]
    outage_raw["_missing_fields"] = outage_raw[critical_cols].isnull().apply(
        lambda row: [c for c, v in row.items() if v], axis=1
    )
    outage_raw["_flagged"] = outage_raw["_missing_fields"].apply(lambda x: len(x) > 0)
    flagged_outages = outage_raw[outage_raw["_flagged"]].copy()
    outage = outage_raw[~outage_raw["_flagged"]].copy()
    print(f"  Outages : {len(outage_raw)} total | {len(outage)} valid | {len(flagged_outages)} flagged")

    # ── Click count sheets ────────────────────────────────────────────────────
    all_clicks = []
    for sheet in xl.sheet_names:
        if "Click Count" in sheet:
            df = xl.parse(sheet)
            df["_source_sheet"] = sheet
            all_clicks.append(df)
            print(f"    {sheet}: {len(df)} rows")

    clicks_raw = pd.concat(all_clicks, ignore_index=True)
    click_critical = ["RepId", "MeterId", "ChangeWindowStart", "ClickCountChange"]
    clicks_raw["_missing_fields"] = clicks_raw[click_critical].isnull().apply(
        lambda row: [c for c, v in row.items() if v], axis=1
    )
    clicks_raw["_flagged"] = clicks_raw["_missing_fields"].apply(lambda x: len(x) > 0)
    flagged_clicks = clicks_raw[clicks_raw["_flagged"]].copy()
    clicks = clicks_raw[~clicks_raw["_flagged"]].copy()
    print(f"  Clicks  : {len(clicks_raw)} total | {len(clicks)} valid | {len(flagged_clicks)} flagged")

    return outage, flagged_outages, clicks, flagged_clicks


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 5 — DATA AGGREGATION
# ════════════════════════════════════════════════════════════════════════════════

def aggregate_data(outage: pd.DataFrame, clicks: pd.DataFrame):
    """
    Collapse raw rows into one summary record per substation and per recloser.
    Adds 'sx' and 'sy' columns (SVG pixel coordinates) to both result DataFrames.

    Returns: (sub_agg, rep_agg)
    """

    # ── Per-substation outage summary ─────────────────────────────────────────
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
        )
        .reset_index()
    )

    # Geo → SVG coords for substation dots
    sub_agg[["lat", "lon"]] = sub_agg.apply(
        lambda r: pd.Series(district_offset(str(r["Sub"]), str(r["District"]))), axis=1
    )
    sub_agg[["sx", "sy"]] = sub_agg.apply(
        lambda r: pd.Series(geo_to_svg(r["lat"], r["lon"])), axis=1
    )

    # ── Per-recloser click summary ─────────────────────────────────────────────
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

    # Normalise to 0–1 for colour mapping (green → yellow → red)
    cmin, cmax = rep_agg["total_clicks"].min(), rep_agg["total_clicks"].max()
    rep_agg["click_score"] = (rep_agg["total_clicks"] - cmin) / max(cmax - cmin, 1)

    # Stable pseudo-random positions within the NE Georgia bounding box.
    # DEBUG: Replace this with a join to a real device location table if available.
    lat_min, lat_max = 33.7, 34.6
    lon_min, lon_max = -84.3, -83.3
    rep_agg = rep_agg.reset_index(drop=True)

    def rep_coords(rep_id):
        h = int(hashlib.md5(str(rep_id).encode()).hexdigest(), 16)
        lat = lat_min + (h % 10000) / 10000 * (lat_max - lat_min)
        lon = lon_min + ((h >> 16) % 10000) / 10000 * (lon_max - lon_min)
        return lat, lon

    coords = rep_agg["RepId"].apply(
        lambda rid: pd.Series(rep_coords(rid), index=["lat", "lon"])
    )
    rep_agg["lat"] = coords["lat"]
    rep_agg["lon"] = coords["lon"]
    rep_agg[["sx", "sy"]] = rep_agg.apply(
        lambda r: pd.Series(geo_to_svg(r["lat"], r["lon"])), axis=1
    )

    print(f"  Substations : {len(sub_agg)}  |  Reclosers : {len(rep_agg)}")
    return sub_agg, rep_agg


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 6 — COLOUR UTILITY
# ════════════════════════════════════════════════════════════════════════════════

def click_color(score: float) -> str:
    """
    Convert a 0–1 score to a hex colour:
        0.0  → #00ff00  (green)
        0.5  → #ffff00  (yellow)
        1.0  → #ff0000  (red)
    """
    r = int(min(255, score * 2 * 255))
    g = int(min(255, (1 - score) * 2 * 255))
    return f"#{r:02x}{g:02x}00"


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SVG MAP GENERATION
# ════════════════════════════════════════════════════════════════════════════════

def build_svg_map() -> str:
    """
    Build a complete SVG string showing Georgia's 159 county outlines.

    Each county in GEORGIA_COUNTIES is converted from (lon, lat) coordinates
    to SVG (x, y) pixel positions and rendered as a <polygon> element.

    NE Georgia service-area counties (NE_GEORGIA_COUNTIES) are drawn with a
    brighter teal fill to make the service territory immediately visible.

    The SVG has three empty <g> layer groups at the bottom that JavaScript
    will populate at runtime:
        #heatCanvas  – canvas-style heatmap drawn via JS (not an actual canvas)
        #dotLayer    – recloser circle markers
        #subLayer    – substation ring markers

    Returns the complete <svg>...</svg> string.

    DEBUG: If counties appear in the wrong positions, check MAP_LAT_MIN/MAX
           and MAP_LON_MIN/MAX — they define the linear mapping from geo to pixels.
    """
    polys = []
    for name, coords in GEORGIA_COUNTIES.items():
        # Convert each (lon, lat) pair to SVG pixel (x, y)
        pts = " ".join(
            f"{geo_to_svg(lat, lon)[0]:.1f},{geo_to_svg(lat, lon)[1]:.1f}"
            for lon, lat in coords
        )
        is_service = name in NE_GEORGIA_COUNTIES
        fill   = "#0e2640" if is_service else "#090f1c"
        stroke = "#1e5080" if is_service else "#12253a"
        sw     = "1.0"     if is_service else "0.6"

        polys.append(
            f'  <polygon points="{pts}" fill="{fill}" stroke="{stroke}" '
            f'stroke-width="{sw}" data-county="{name}"/>'
        )

    return (
        f'<svg id="gaMap" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'style="position:absolute;inset:0;width:100%;height:100%">\n'
        + "\n".join(polys)
        + "\n  <!-- JS-populated data layers -->"
        + "\n  <g id='dotLayer'></g>"
        + "\n  <g id='subLayer'></g>"
        + "\n</svg>"
    )


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 8 — HTML GENERATION
# ════════════════════════════════════════════════════════════════════════════════

def build_html(sub_agg, rep_agg, flagged_outages, flagged_clicks, svg_map) -> str:
    """
    Assemble and return the complete self-contained HTML string.

    Architecture of the output file:
        <head>  CSS design system (all variables, layout, components)
        <body>
          header          Summary stat chips (counts injected by Python)
          #sidebar        4-tab panel: Controls / Reclosers / Outages / Flagged
          #mapWrap        SVG map (county polygons) + canvas heatmap overlay
          <script>
            JSON data constants (RECLOSERS, SUBSTATIONS, FLAGGED_OUT, FLAGGED_CLK)
            All rendering and UI logic (dots, heatmap, tooltips, popups, filters)

    No CDN imports. No external files. Opens in any browser, offline.
    """

    def safe_str(v):
        """Safely convert any value to a JSON-safe string."""
        if pd.isna(v):
            return "N/A"
        if isinstance(v, (pd.Timestamp, datetime)):
            return v.strftime("%Y-%m-%d %H:%M")
        return str(v)

    # ── Serialise all data to JSON ─────────────────────────────────────────────
    reclosers_js = [
        {
            "repId": int(r["RepId"]),
            "sx": round(float(r["sx"]), 1),
            "sy": round(float(r["sy"]), 1),
            "totalClicks": int(r["total_clicks"]),
            "eventCount": int(r["event_count"]),
            "avgClicks": round(float(r["avg_clicks_per_window"]), 1),
            "maxWindow": int(r["max_window_clicks"]),
            "firstSeen": safe_str(r["first_seen"]),
            "lastSeen": safe_str(r["last_seen"]),
            "score": round(float(r["click_score"]), 4),
            "color": click_color(float(r["click_score"])),
        }
        for _, r in rep_agg.iterrows()
    ]

    substations_js = [
        {
            "sub": safe_str(r["Sub"]),
            "district": safe_str(r["District"]),
            "sx": round(float(r["sx"]), 1),
            "sy": round(float(r["sy"]), 1),
            "outageCount": int(r["outage_count"]),
            "customerMinutes": int(r["total_customer_minutes"]),
            "avgDuration": round(float(r["avg_duration_hrs"]), 2),
            "customersAffected": int(r["customers_affected"]),
            "causes": r["causes"],
        }
        for _, r in sub_agg.iterrows()
    ]

    flagged_out_js = [
        {"outage": safe_str(r.get("Outage")), "missing": r["_missing_fields"],
         "timeOff": safe_str(r.get("Time Off")), "district": safe_str(r.get("District"))}
        for _, r in flagged_outages.iterrows()
    ]
    flagged_clk_js = [
        {"repId": safe_str(r.get("RepId")), "missing": r["_missing_fields"],
         "sheet": safe_str(r.get("_source_sheet"))}
        for _, r in flagged_clicks.iterrows()
    ]

    rj  = json.dumps(reclosers_js)
    sj  = json.dumps(substations_js)
    foj = json.dumps(flagged_out_js)
    fcj = json.dumps(flagged_clk_js)

    total_clicks   = int(rep_agg["total_clicks"].sum())
    total_outages  = int(sub_agg["outage_count"].sum())
    total_cust_min = int(sub_agg["total_customer_minutes"].sum())
    flag_count     = len(flagged_outages) + len(flagged_clicks)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Georgia Grid Monitor — Offline</title>
<style>
/* ══ Design tokens ═════════════════════════════════════════════════════════ */
:root {{
  --bg:     #060e18;   /* Page / map background             */
  --panel:  #0b1522;   /* Sidebar and header background     */
  --border: #14263a;   /* Dividers and card borders         */
  --accent: #00c8ff;   /* Primary interactive cyan          */
  --warn:   #ff6b35;   /* Flagged / warning orange          */
  --text:   #cfe0ef;   /* Body text                         */
  --muted:  #486070;   /* Secondary labels                  */
  --green:  #00e676;   /* LOW badge                         */
  --red:    #ff1744;   /* HIGH badge                        */
  --yellow: #ffd600;   /* MED badge                         */
  --r:      6px;       /* Border radius                     */
}}

* {{ box-sizing: border-box; margin: 0; padding: 0; }}

body {{
  font-family: 'Segoe UI', system-ui, sans-serif;
  background: var(--bg);
  color: var(--text);
  height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}}

/* ══ Header ════════════════════════════════════════════════════════════════ */
header {{
  height: 52px;
  background: var(--panel);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  padding: 0 18px;
  gap: 16px;
  flex-shrink: 0;
  z-index: 10;
}}

header h1 {{
  font-size: 13px;
  font-weight: 700;
  color: var(--accent);
  letter-spacing: 3px;
  text-transform: uppercase;
  white-space: nowrap;
}}

.hstats {{ display: flex; gap: 16px; margin-left: auto; flex-wrap: wrap; }}
.hs {{ font-size: 11px; color: var(--muted); letter-spacing: .8px; }}
.hs b {{ color: var(--accent); }}

/* ══ Layout ════════════════════════════════════════════════════════════════ */
.layout {{
  display: flex;
  flex: 1;
  overflow: hidden;
}}

/* ══ Sidebar ═══════════════════════════════════════════════════════════════ */
#sb {{
  width: 285px;
  background: var(--panel);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  overflow: hidden;
}}

/* Tab bar */
.tabs {{ display: flex; border-bottom: 1px solid var(--border); }}
.tab {{
  flex: 1; padding: 9px 3px; background: none; border: none;
  border-bottom: 2px solid transparent; color: var(--muted);
  font-size: 9px; letter-spacing: 1px; cursor: pointer; transition: .15s;
  text-transform: uppercase;
}}
.tab.on {{ color: var(--accent); border-bottom-color: var(--accent); }}
.tab:hover:not(.on) {{ color: var(--text); }}

/* Tab panes */
.pane {{ display: none; flex: 1; overflow-y: auto; padding: 11px; }}
.pane.on {{ display: block; }}

/* Section label */
.clabel {{
  font-size: 9px; letter-spacing: 1.5px; color: var(--muted);
  text-transform: uppercase; margin-bottom: 6px;
}}

.csect {{ margin-bottom: 13px; }}

/* Toggle row */
.trow {{
  display: flex; align-items: center; justify-content: space-between;
  padding: 6px 0; border-bottom: 1px solid var(--border);
}}
.trow span {{ font-size: 12px; }}

/* Toggle switch (CSS-only, no library) */
.tog {{ position: relative; width: 34px; height: 17px; cursor: pointer; }}
.tog input {{ opacity: 0; width: 0; height: 0; }}
.tsl {{
  position: absolute; inset: 0; background: var(--border);
  border-radius: 17px; transition: .18s;
}}
.tsl::before {{
  content: ''; position: absolute;
  width: 11px; height: 11px; left: 3px; top: 3px;
  background: var(--muted); border-radius: 50%; transition: .18s;
}}
.tog input:checked + .tsl {{ background: rgba(0,200,255,.2); }}
.tog input:checked + .tsl::before {{ transform: translateX(17px); background: var(--accent); }}

/* Range sliders */
.srow {{ padding: 6px 0; border-bottom: 1px solid var(--border); }}
.srow input[type=range] {{ width: 100%; accent-color: var(--accent); margin-top: 5px; }}
.sv {{ font-size: 10px; color: var(--accent); float: right; }}

/* Colour legend bar */
.lbar {{ height: 8px; border-radius: 4px; background: linear-gradient(to right,#00ff00,#ffff00,#ff0000); margin: 5px 0 3px; }}
.llbls {{ display: flex; justify-content: space-between; font-size: 9px; color: var(--muted); }}

/* District select */
select {{
  width: 100%; padding: 5px 7px; background: var(--bg);
  border: 1px solid var(--border); border-radius: 4px;
  color: var(--text); font-size: 11px; cursor: pointer;
}}

/* List cards (Reclosers / Outages tabs) */
.card {{
  background: var(--bg); border: 1px solid var(--border);
  border-radius: var(--r); padding: 8px; margin-bottom: 6px;
  cursor: pointer; transition: border-color .15s;
}}
.card:hover {{ border-color: var(--accent); }}
.cn {{ font-size: 11px; font-weight: 600; color: var(--accent); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.cm {{ font-size: 10px; color: var(--muted); margin-top: 2px; }}

/* Severity badges */
.badge {{ display: inline-block; padding: 1px 5px; border-radius: 3px; font-size: 9px; font-weight: 700; margin-right: 3px; }}
.b-r {{ background: rgba(255,23,68,.16);  color: var(--red);    }}
.b-y {{ background: rgba(255,214,0,.12); color: var(--yellow); }}
.b-g {{ background: rgba(0,230,118,.12); color: var(--green);  }}

/* Flagged cards */
.fcard {{
  background: rgba(255,107,53,.04); border: 1px solid rgba(255,107,53,.22);
  border-radius: var(--r); padding: 7px 9px; margin-bottom: 5px; font-size: 10px;
}}
.ft {{ color: var(--warn); margin-bottom: 2px; }}
.fm {{ color: var(--muted); }}

/* ══ Map wrapper ═══════════════════════════════════════════════════════════ */
#mw {{
  flex: 1; position: relative; overflow: hidden; background: var(--bg);
}}

/* Heatmap canvas — absolutely positioned on top of SVG */
#hc {{
  position: absolute; inset: 0;
  width: 100%; height: 100%;
  pointer-events: none;
  opacity: 0; transition: opacity .3s;
}}
#hc.on {{ opacity: 0.72; }}

/* ══ Tooltip ═══════════════════════════════════════════════════════════════ */
#tip {{
  position: absolute; pointer-events: none; display: none;
  background: var(--panel); border: 1px solid var(--accent);
  border-radius: 4px; padding: 4px 9px; font-size: 11px;
  white-space: nowrap; z-index: 40;
  box-shadow: 0 0 10px rgba(0,200,255,.18);
}}

/* ══ Popup ═════════════════════════════════════════════════════════════════ */
#pop {{
  position: absolute; display: none; z-index: 50; min-width: 210px;
  background: var(--panel); border: 1px solid var(--accent);
  border-radius: var(--r); box-shadow: 0 0 20px rgba(0,200,255,.22);
}}

/* Popup header */
.ph {{
  padding: 9px 11px 6px;
  font-size: 11px; font-weight: 700; color: var(--accent); letter-spacing: 1px;
  border-bottom: 1px solid var(--border);
  display: flex; justify-content: space-between; align-items: center;
}}
.px {{ background: none; border: none; color: var(--muted); cursor: pointer; font-size: 13px; line-height: 1; }}
.px:hover {{ color: var(--text); }}

/* Popup body */
.pb {{ padding: 7px 11px 9px; }}
.pr {{
  display: flex; justify-content: space-between;
  padding: 3px 0; border-bottom: 1px solid var(--border); font-size: 11px;
}}
.pr:last-child {{ border-bottom: none; }}
.pk {{ color: var(--muted); }}
.pv {{ color: var(--text); font-weight: 600; }}

/* Expandable advanced section */
.padv {{ display: none; margin-top: 5px; padding-top: 5px; border-top: 1px solid var(--border); }}
.padv.on {{ display: block; }}
.ptog {{
  width: 100%; margin-top: 6px; padding: 4px; background: transparent;
  border: 1px solid var(--border); border-radius: 3px; color: var(--muted);
  font-size: 10px; cursor: pointer; letter-spacing: 1px; transition: .15s;
}}
.ptog:hover {{ border-color: var(--accent); color: var(--accent); }}

/* Scrollbar */
::-webkit-scrollbar {{ width: 3px; }}
::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 2px; }}

/* Small hint label in bottom-right of map */
#mapNote {{
  position: absolute; bottom: 8px; right: 10px;
  font-size: 9px; color: var(--muted); letter-spacing: .5px; pointer-events: none;
}}
</style>
</head>
<body>

<!-- ═══ HEADER ═══════════════════════════════════════════════════════════════ -->
<header>
  <h1>⚡ Georgia Grid Monitor</h1>
  <div class="hstats">
    <div class="hs">RECLOSERS <b>{len(rep_agg)}</b></div>
    <div class="hs">OUTAGES <b>{total_outages}</b></div>
    <div class="hs">CUST·MIN <b>{total_cust_min:,}</b></div>
    <div class="hs">CLICKS <b>{total_clicks:,}</b></div>
    <div class="hs">FLAGGED <b style="color:var(--warn)">{flag_count}</b></div>
  </div>
</header>

<div class="layout">

  <!-- ═══ SIDEBAR ══════════════════════════════════════════════════════════ -->
  <div id="sb">
    <div class="tabs">
      <button class="tab on"  onclick="tab('ctrl')">Controls</button>
      <button class="tab"     onclick="tab('rec')">Reclosers</button>
      <button class="tab"     onclick="tab('out')">Outages</button>
      <button class="tab"     onclick="tab('flag')">Flagged</button>
    </div>

    <!-- Controls tab -->
    <div id="p-ctrl" class="pane on">

      <div class="csect">
        <div class="clabel">Layers</div>
        <div class="trow"><span>Recloser Dots</span>
          <label class="tog"><input type="checkbox" id="tog-rec" checked onchange="setLayer('rec',this.checked)"><span class="tsl"></span></label>
        </div>
        <div class="trow"><span>Substations</span>
          <label class="tog"><input type="checkbox" id="tog-sub" checked onchange="setLayer('sub',this.checked)"><span class="tsl"></span></label>
        </div>
        <div class="trow"><span>Click Heatmap</span>
          <label class="tog"><input type="checkbox" id="tog-hm" onchange="setHeat(this.checked)"><span class="tsl"></span></label>
        </div>
      </div>

      <div class="csect">
        <div class="clabel">Min Click Count &nbsp;<span class="sv" id="sv-mc">0</span></div>
        <div class="srow">
          <input type="range" id="sl-mc" min="0" max="400" value="0" oninput="setMinClicks(+this.value)">
        </div>
      </div>

      <div class="csect">
        <div class="clabel">Dot Size &nbsp;<span class="sv" id="sv-ds">5</span></div>
        <div class="srow">
          <input type="range" id="sl-ds" min="2" max="16" value="5" oninput="setDotSize(+this.value)">
        </div>
      </div>

      <div class="csect">
        <div class="clabel">Colour Legend</div>
        <div class="lbar"></div>
        <div class="llbls"><span>Low</span><span>Medium</span><span>High</span></div>
        <div style="font-size:10px;color:var(--muted);margin-top:7px;line-height:1.6">
          Coloured dots = reclosers by total click count.<br>
          Cyan rings = substations, sized by outages.<br>
          Highlighted counties = NE Georgia service area.
        </div>
      </div>

      <div class="csect">
        <div class="clabel">District Filter</div>
        <select id="sel-dist" onchange="setDistrict(this.value)">
          <option value="all">All Districts</option>
          <option value="Gainesville">Gainesville</option>
          <option value="Jefferson">Jefferson</option>
          <option value="Lawrenceville">Lawrenceville</option>
          <option value="Neese">Neese</option>
        </select>
      </div>
    </div>

    <!-- Reclosers tab -->
    <div id="p-rec" class="pane"><div id="rec-list"></div></div>

    <!-- Outages tab -->
    <div id="p-out" class="pane"><div id="out-list"></div></div>

    <!-- Flagged tab -->
    <div id="p-flag" class="pane">
      <div id="flag-count" style="font-size:10px;color:var(--warn);margin-bottom:8px;letter-spacing:.8px;"></div>
      <div id="flag-list"></div>
    </div>
  </div>

  <!-- ═══ MAP AREA ══════════════════════════════════════════════════════════ -->
  <div id="mw">
    <!-- Offline SVG map — all 159 Georgia counties, no internet required -->
    {svg_map}
    <!-- Heatmap canvas — drawn by JavaScript using Canvas 2D API -->
    <canvas id="hc"></canvas>
    <!-- Hover tooltip -->
    <div id="tip"></div>
    <!-- Click popup -->
    <div id="pop">
      <div class="ph">
        <span id="pop-title"></span>
        <button class="px" onclick="closePop()">✕</button>
      </div>
      <div class="pb" id="pop-body"></div>
    </div>
    <div id="mapNote">Fully offline — no internet required</div>
  </div>
</div>

<script>
// ════════════════════════════════════════════════════════════════════════════
// DATA — injected by Python at build time.
// All four constants are plain JS arrays. Inspect in browser console (F12).
// ════════════════════════════════════════════════════════════════════════════
const RECLOSERS   = {rj};
const SUBSTATIONS = {sj};
const FLAGGED_OUT = {foj};
const FLAGGED_CLK = {fcj};

// SVG viewport dimensions (must match Python SVG_WIDTH / SVG_HEIGHT)
const SVG_W = {SVG_WIDTH}, SVG_H = {SVG_HEIGHT};

// ── State variables ────────────────────────────────────────────────────────
let dotR      = 5;       // Current dot radius in SVG units
let minClicks = 0;       // Min-click-count filter threshold
let district  = 'all';  // District filter value
let showRec   = true;    // Show recloser dots?
let showSub   = true;    // Show substation rings?

// ── SVG namespace shorthand ────────────────────────────────────────────────
const NS = 'http://www.w3.org/2000/svg';
const mk = (tag, a) => {{ const e = document.createElementNS(NS, tag); Object.entries(a).forEach(([k,v]) => e.setAttribute(k,v)); return e; }};

// ════════════════════════════════════════════════════════════════════════════
// SVG COORDINATE HELPER
// The SVG uses a fixed viewBox but scales to fill #mw.  To position overlays
// (tooltip, popup) correctly we need the CSS pixel position of any SVG point.
// ════════════════════════════════════════════════════════════════════════════
function svgPx(sx, sy) {{
  const rect  = document.getElementById('mw').getBoundingClientRect();
  // SVG scales with preserveAspectRatio="xMidYMid meet" by default
  const scale = Math.min(rect.width / SVG_W, rect.height / SVG_H);
  const offX  = (rect.width  - SVG_W * scale) / 2;
  const offY  = (rect.height - SVG_H * scale) / 2;
  return [offX + sx * scale, offY + sy * scale];
}}

// ════════════════════════════════════════════════════════════════════════════
// DOT RENDERING
// Draws recloser circles (coloured by click score) and substation rings
// (blue, sized by outage count) as SVG <circle> elements.
// Called on init and whenever any filter/size control changes.
// ════════════════════════════════════════════════════════════════════════════
function buildDots() {{
  document.getElementById('dotLayer').innerHTML = '';
  document.getElementById('subLayer').innerHTML = '';

  // ── Recloser dots ─────────────────────────────────────────────────────────
  if (showRec) {{
    RECLOSERS.forEach(r => {{
      if (r.totalClicks < minClicks) return;  // Apply min-click filter
      const c = mk('circle', {{
        cx: r.sx, cy: r.sy, r: dotR,
        fill: r.color,
        stroke: r.color,
        'stroke-width': r.score > 0.7 ? 1.5 : 0.4,
        opacity: 0.88,
        style: 'cursor:pointer'
      }});
      c.addEventListener('mouseenter', e => showTip(e, `Rep ${{r.repId}} — ${{r.totalClicks}} clicks`));
      c.addEventListener('mouseleave', hideTip);
      c.addEventListener('click', e => {{ e.stopPropagation(); showRecPop(r, e); }});
      document.getElementById('dotLayer').appendChild(c);
    }});
  }}

  // ── Substation rings ──────────────────────────────────────────────────────
  if (showSub) {{
    SUBSTATIONS.forEach(s => {{
      if (district !== 'all' && s.district !== district) return;
      const rad = Math.min(5 + s.outageCount * 0.7, 18);  // Scale by outage count
      const c = mk('circle', {{
        cx: s.sx, cy: s.sy, r: rad,
        fill: 'rgba(0,200,255,0.10)',
        stroke: '#00c8ff',
        'stroke-width': 1.1,
        opacity: 0.85,
        style: 'cursor:pointer'
      }});
      c.addEventListener('mouseenter', e => showTip(e, `${{s.sub}} — ${{s.outageCount}} outages`));
      c.addEventListener('mouseleave', hideTip);
      c.addEventListener('click', e => {{ e.stopPropagation(); showSubPop(s, e); }});
      document.getElementById('subLayer').appendChild(c);
    }});
  }}
}}

// ════════════════════════════════════════════════════════════════════════════
// HEATMAP — Canvas 2D radial gradient overlay (no library)
// Each recloser contributes a radial gradient weighted by its click score.
// The canvas sits above the SVG (z-order) but has pointer-events:none so
// clicks still pass through to the SVG dots underneath.
// ════════════════════════════════════════════════════════════════════════════
function buildHeat() {{
  const mw   = document.getElementById('mw');
  const hc   = document.getElementById('hc');
  const rect = mw.getBoundingClientRect();

  hc.width  = rect.width;
  hc.height = rect.height;

  const ctx   = hc.getContext('2d');
  ctx.clearRect(0, 0, hc.width, hc.height);

  // Compute SVG-to-canvas transform (same as svgPx)
  const scale = Math.min(rect.width / SVG_W, rect.height / SVG_H);
  const offX  = (rect.width  - SVG_W * scale) / 2;
  const offY  = (rect.height - SVG_H * scale) / 2;

  // Draw each recloser as a soft radial gradient blob
  RECLOSERS.filter(r => r.totalClicks >= minClicks).forEach(r => {{
    const cx = offX + r.sx * scale;
    const cy = offY + r.sy * scale;
    const rad = 24 + r.score * 22;   // Bigger radius = more spread

    const cr = Math.min(255, Math.round(r.score * 2 * 255));  // Red channel
    const cg = Math.min(255, Math.round((1 - r.score) * 2 * 255)); // Green channel

    const g = ctx.createRadialGradient(cx, cy, 0, cx, cy, rad);
    g.addColorStop(0,   `rgba(${{cr}},${{cg}},0,0.6)`);
    g.addColorStop(0.5, `rgba(${{cr}},${{cg}},0,0.2)`);
    g.addColorStop(1,   `rgba(${{cr}},${{cg}},0,0)`);

    ctx.fillStyle = g;
    ctx.beginPath();
    ctx.arc(cx, cy, rad, 0, Math.PI * 2);
    ctx.fill();
  }});
}}

// ════════════════════════════════════════════════════════════════════════════
// TOOLTIP
// ════════════════════════════════════════════════════════════════════════════
function showTip(e, txt) {{
  const t = document.getElementById('tip');
  t.textContent = txt;
  t.style.display = 'block';
  posTip(e);
}}

function posTip(e) {{
  const t    = document.getElementById('tip');
  const rect = document.getElementById('mw').getBoundingClientRect();
  let x = e.clientX - rect.left + 12;
  let y = e.clientY - rect.top  + 12;
  if (x + 220 > rect.width)  x -= 200;
  if (y + 36  > rect.height) y -= 38;
  t.style.left = x + 'px';
  t.style.top  = y + 'px';
}}

function hideTip() {{ document.getElementById('tip').style.display = 'none'; }}

// ════════════════════════════════════════════════════════════════════════════
// POPUPS
// Each dot click opens a popup with basic info and an expandable advanced panel.
// ════════════════════════════════════════════════════════════════════════════
const pr  = (k, v) => `<div class="pr"><span class="pk">${{k}}</span><span class="pv">${{v}}</span></div>`;
const bdg = s => s > 0.7 ? '<span class="badge b-r">HIGH</span>'
              : s > 0.35 ? '<span class="badge b-y">MED</span>'
              : '<span class="badge b-g">LOW</span>';

function showRecPop(r, e) {{
  document.getElementById('pop-title').innerHTML = `⚡ RECLOSER ${{r.repId}}`;
  document.getElementById('pop-body').innerHTML = `
    ${{pr('Status', bdg(r.score))}}
    ${{pr('Total Clicks', r.totalClicks)}}
    ${{pr('Event Windows', r.eventCount)}}
    <div class="padv" id="padv">
      ${{pr('Avg Clicks / Window', r.avgClicks)}}
      ${{pr('Peak Window', r.maxWindow)}}
      ${{pr('First Seen', r.firstSeen)}}
      ${{pr('Last Seen', r.lastSeen)}}
      ${{pr('Score', (r.score*100).toFixed(1)+'%')}}
    </div>
    <button class="ptog" onclick="togAdv(this)">▼ Show Advanced</button>`;
  posPop(e);
}}

function showSubPop(s, e) {{
  document.getElementById('pop-title').innerHTML = `🏭 ${{s.sub}}`;
  const cRows = Object.entries(s.causes||{{}})
    .map(([k,v]) => pr(k.slice(0,28), v+'x')).join('');
  document.getElementById('pop-body').innerHTML = `
    ${{pr('District', s.district)}}
    ${{pr('Outages', s.outageCount)}}
    ${{pr('Customers Affected', s.customersAffected)}}
    ${{pr('Customer·Minutes', s.customerMinutes.toLocaleString())}}
    ${{pr('Avg Duration (hrs)', s.avgDuration)}}
    <div class="padv" id="padv">
      <div style="font-size:9px;color:var(--muted);margin-bottom:3px;letter-spacing:1px">TOP CAUSES</div>
      ${{cRows || '<div style="color:var(--muted);font-size:10px">No cause data</div>'}}
    </div>
    <button class="ptog" onclick="togAdv(this)">▼ Show Causes</button>`;
  posPop(e);
}}

function posPop(e) {{
  const pop  = document.getElementById('pop');
  const rect = document.getElementById('mw').getBoundingClientRect();
  pop.style.display = 'block';
  // Delay measurement until the popup has rendered and has a height
  setTimeout(() => {{
    let x = e.clientX - rect.left + 14;
    let y = e.clientY - rect.top  - 15;
    const pw = pop.offsetWidth  || 220;
    const ph = pop.offsetHeight || 180;
    if (x + pw > rect.width  - 8)  x = e.clientX - rect.left - pw - 14;
    if (y + ph > rect.height - 8)  y = rect.height - ph - 8;
    if (y < 4) y = 4;
    pop.style.left = x + 'px';
    pop.style.top  = y + 'px';
  }}, 0);
}}

function closePop() {{ document.getElementById('pop').style.display = 'none'; }}

// Clicking the map background closes any open popup
document.getElementById('mw').addEventListener('click', closePop);

function togAdv(btn) {{
  const adv = document.getElementById('padv');
  adv.classList.toggle('on');
  btn.textContent = adv.classList.contains('on') ? '▲ Hide' : '▼ Show Advanced';
}}

// ════════════════════════════════════════════════════════════════════════════
// CONTROL CALLBACKS
// Called from HTML oninput / onchange attributes.
// ════════════════════════════════════════════════════════════════════════════
function setLayer(which, on) {{
  if (which === 'rec') showRec = on;
  if (which === 'sub') showSub = on;
  buildDots();
}}

function setHeat(on) {{
  const hc = document.getElementById('hc');
  if (on) {{ buildHeat(); hc.classList.add('on'); }}
  else    {{ hc.classList.remove('on'); }}
}}

function setMinClicks(v) {{
  minClicks = v;
  document.getElementById('sv-mc').textContent = v;
  buildDots();
  if (document.getElementById('tog-hm').checked) buildHeat();
}}

function setDotSize(v) {{
  dotR = v;
  document.getElementById('sv-ds').textContent = v;
  buildDots();
}}

function setDistrict(v) {{
  district = v;
  buildDots();
}}

// ════════════════════════════════════════════════════════════════════════════
// SIDEBAR LIST BUILDERS
// Populate the Reclosers, Outages, and Flagged tab panes.
// ════════════════════════════════════════════════════════════════════════════
function buildRecList() {{
  const sorted = [...RECLOSERS].sort((a,b) => b.totalClicks - a.totalClicks);
  document.getElementById('rec-list').innerHTML = sorted.map(r => `
    <div class="card">
      <div class="cn">${{bdg(r.score)}} Rep ${{r.repId}}</div>
      <div class="cm">Clicks: ${{r.totalClicks}} &nbsp;|&nbsp; Windows: ${{r.eventCount}}</div>
    </div>`).join('');
}}

function buildOutList() {{
  const sorted = [...SUBSTATIONS].sort((a,b) => b.outageCount - a.outageCount);
  document.getElementById('out-list').innerHTML = sorted.map(s => `
    <div class="card">
      <div class="cn">🏭 ${{s.sub}}</div>
      <div class="cm">${{s.district}} &nbsp;|&nbsp; ${{s.outageCount}} outages &nbsp;|&nbsp; ${{s.customersAffected}} cust.</div>
    </div>`).join('');
}}

function buildFlagList() {{
  const total = FLAGGED_OUT.length + FLAGGED_CLK.length;
  document.getElementById('flag-count').textContent = total + ' entries flagged and excluded';
  let html = '';
  if (FLAGGED_OUT.length) {{
    html += `<div class="clabel" style="margin-bottom:5px">Outage data (${{FLAGGED_OUT.length}})</div>`;
    html += FLAGGED_OUT.map(f => `
      <div class="fcard">
        <div class="ft">Outage #${{f.outage}}</div>
        <div class="fm">Missing: ${{f.missing.join(', ')}}</div>
        <div class="fm">Date: ${{f.timeOff}}</div>
      </div>`).join('');
  }}
  if (FLAGGED_CLK.length) {{
    html += `<div class="clabel" style="margin:8px 0 5px">Click data (${{FLAGGED_CLK.length}})</div>`;
    html += FLAGGED_CLK.map(f => `
      <div class="fcard">
        <div class="ft">RepId: ${{f.repId}}</div>
        <div class="fm">Missing: ${{f.missing.join(', ')}}</div>
        <div class="fm">Sheet: ${{f.sheet}}</div>
      </div>`).join('');
  }}
  document.getElementById('flag-list').innerHTML = html;
}}

// ════════════════════════════════════════════════════════════════════════════
// TAB SWITCHING
// ════════════════════════════════════════════════════════════════════════════
function tab(name) {{
  const ids = ['ctrl','rec','out','flag'];
  document.querySelectorAll('.tab').forEach((b,i)  => b.classList.toggle('on', ids[i]===name));
  document.querySelectorAll('.pane').forEach(p     => p.classList.toggle('on', p.id==='p-'+name));
}}

// ════════════════════════════════════════════════════════════════════════════
// INIT — called once when page loads
// ════════════════════════════════════════════════════════════════════════════
buildDots();
buildRecList();
buildOutList();
buildFlagList();
</script>
</body>
</html>"""


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 9 — ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

def main():
    """
    Full pipeline:
        1. load_data      → read + validate Excel
        2. aggregate_data → summarise per substation / per recloser
        3. build_svg_map  → render Georgia counties as inline SVG
        4. build_html     → assemble self-contained HTML
        5. write file     → save to disk

    Override defaults with command-line args:
        python3 generate_map.py [input.xlsx] [output.html]
    """
    path   = sys.argv[1] if len(sys.argv) > 1 else EXCEL_PATH
    output = sys.argv[2] if len(sys.argv) > 2 else OUTPUT_HTML

    print(f"\n{'='*55}")
    print(" Georgia Grid Monitor — Fully Offline Map Generator")
    print(f"{'='*55}")
    print(f"Input:  {path}\nOutput: {output}\n")

    outage, flagged_outages, clicks, flagged_clicks = load_data(path)
    sub_agg, rep_agg = aggregate_data(outage, clicks)

    print("  Building Georgia county SVG...")
    svg_map = build_svg_map()

    print("  Assembling HTML...")
    html = build_html(sub_agg, rep_agg, flagged_outages, flagged_clicks, svg_map)

    with open(output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✓  {output}  ({os.path.getsize(output)//1024} KB)")
    print("   Open in any browser — works 100% offline.\n")


if __name__ == "__main__":
    main()