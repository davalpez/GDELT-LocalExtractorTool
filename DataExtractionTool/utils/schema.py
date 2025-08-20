## Variable file where we store the values for GDELTv1 columns ##

COLUMNS_TO_KEEP_LARGE = [
    'GLOBALEVENTID',        # Globally unique identifier for the event record.
    'SQLDATE',              # Date the event occurred (YYYYMMDD format).
    'Actor1Code',           # Raw CAMEO code for Actor 1 (the primary actor). Can be composite (e.g., 'USAELI' for US elite).
    'Actor1Name',           # Best available English name for Actor 1.
    'Actor1CountryCode',    # 3-character CAMEO country code for Actor 1 (e.g., 'USA', 'RUS').
    'Actor1Geo_CountryCode',# 2-character FIPS 10-4 country code for Actor 1's location.
    'Actor1Geo_FullName',   # Full human-readable name for Actor 1's location.
    'Actor1KnownGroupCode', # Specific group code if Actor 1 is a known non-state group (e.g., 'REB' for Rebels).
    'Actor2Code',           # Raw CAMEO code for Actor 2 (the actor being acted upon).
    'Actor2Name',           # Best available English name for Actor 2.
    'Actor2CountryCode',    # 3-character CAMEO country code for Actor 2.
    'Actor2KnownGroupCode', # Specific group code if Actor 2 is a known non-state group.
    'Actor2Geo_CountryCode',# 2-character FIPS 10-4 country code for Actor 2's location.
    'Actor2Geo_FullName',   # Full human-readable name for Actor 2's location.
    'IsRootEvent',          # Flag (1 or 0) indicating if this event is considered the "root" event in a potentially linked chain of events. 1 = Root, 0 = Not Root.
    'EventCode',            # Full 4-digit CAMEO event code describing the action (e.g., '010' for Make Public Statement).
    'EventBaseCode',        # 2-digit base CAMEO event code (e.g., '01' for Make statement).
    'EventRootCode',        # 1-digit root CAMEO event code (e.g., '1' for Verbal Cooperation).
    'QuadClass',            # Quad classification of the event: 1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict.
    'GoldsteinScale',       # Goldstein scale score (-10 to +10) indicating the theoretical potential impact/intensity of the event.
    'NumMentions',          # Total number of source documents mentioning this event.
    'NumSources',           # Total number of distinct news sources mentioning this event.
    'NumArticles',          # Total number of articles (often same as NumMentions in V1) mentioning this event.
    'AvgTone',              # Average sentiment tone of the articles mentioning this event (-100 to +100).
    'ActionGeo_Type',       # Geolocation type for the location where the action took place. (Same codes as Actor1Geo_Type).
    'ActionGeo_FullName',   # Full human-readable name for the action's location.
    'ActionGeo_CountryCode',# 2-character FIPS 10-4 country code for the action's location.
    'DATEADDED',            # Timestamp (YYYYMMDDHHMMSS) when the event record was added to the GDELT database.
    'SOURCEURL'             # URL of the source document from which the event was extracted.
]

COLUMNS_TO_KEEP_SHORT = [
    'GLOBALEVENTID',        # Globally unique identifier for the event record.
    'SQLDATE',              # Date the event occurred (YYYYMMDD format).
    'Actor1Name',           # # Best available English name for Actor 1.
    'Actor1Geo_FullName',   # Full human-readable name for Actor 1's location.
    'Actor2Name',           # Best available English name for Actor 2.
    'Actor2Geo_FullName',   # Full human-readable name for Actor 2's location.
    'IsRootEvent',          # Flag (1 or 0) indicating if this event is considered the "root" event in a potentially linked chain of events. 1 = Root, 0 = Not Root.
    'EventCode',            # Full 4-digit CAMEO event code describing the action (e.g., '010' for Make Public Statement).
    'EventBaseCode',        # 2-digit base CAMEO event code (e.g., '01' for Make statement).
    'QuadClass',            # Quad classification of the event: 1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict.
    'GoldsteinScale',       # Goldstein scale score (-10 to +10) indicating the theoretical potential impact/intensity of the event.
    'NumMentions',          # Total number of source documents mentioning this event.
    'NumSources',           # Total number of distinct news sources mentioning this event.
    'NumArticles',          # Total number of articles (often same as NumMentions in V1) mentioning this event.
    'AvgTone',              # Average sentiment tone of the articles mentioning this event (-100 to +100).
    'ActionGeo_Type',       # Geolocation type for the location where the action took place. (Same codes as Actor1Geo_Type).
    'ActionGeo_FullName',   # Full human-readable name for the action's location.
    'ActionGeo_CountryCode',# 2-character FIPS 10-4 country code for the action's location.
    'SOURCEURL'             # URL of the source document from which the event was extracted.
]

