# Predefined Ports:
CHORD_PORT = 8000
SCRAP_PORT = 8001
BEACON_PORT = 8010

# FLAGS!
# Stablish initial communications between chord node and scrapper
REQ_ASOC_SCRAP = b"req_asoc"
REP_ASOC_SCRAP_YES = b"rep_asoc_yes"
REP_ASOC_SCRAP_NO = b"rep_asoc_no"
REP_ASOC_SCRAP_ALR = b"rep_asoc_already"

# Ask to scrap certain url ...
REQ_SCRAP_URL = b"req_scrap_url"
REP_SCRAP_URL = b"rep_scrap_url"
