#!/usr/bin/env python3
"""
Central configuration for ContentSquare scripts.
"""

####### GENERAL PARAMS

# Segment IDs to analyze. Keep empty to skip segment-level section.
SEGMENT_IDS_TO_ANALYZE = [6383684, 6383688, 6383692]

# Whether to show metrics split by desktop/mobile/tablet.
ANALYZE_BY_DEVICE = True

# Analysis period (UTC), inclusive.
# Expected format: "YYYY-MM-DD"
START_DATE = "2026-02-01"
END_DATE = "2026-03-01"

####### PAGE GROUP METRICS PARAMS

# Page-group ID for page metrics mode.
# Leave empty (None) to use site metrics endpoints instead.
PAGE_GROUP_ID = 52516480

# Mapping ID used to filter page groups export in app_cs.py.
PAGE_GROUP_MAPPING_ID = 2066672

GOAL_IDS = [1816096]
