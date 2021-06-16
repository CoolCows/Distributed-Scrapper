# Predefined Ports:
CHORD_PORT = 8000
SCRAP_PORT = 8001
CHORD_BEACON_PORT = 8010
SCRAP_BEACON_PORT = 8011

CODE_WORD_CHORD = b"chord"
CODE_WORD_SCRAP = b"scrapper"

# FLAGS!
# Stablish initial communications between chord node and scrapper
REQ_SCRAP_ASOC = b"req_asoc"
REP_SCRAP_ASOC_YES = b"rep_asoc_yes"
REP_SCRAP_ASOC_NO = b"rep_asoc_no"

# Ask to scrap certain url ...
REQ_SCRAP_URL = b"req_scrap_url"
REP_SCRAP_URL = b"rep_scrap_url"

# Ask for ack
REQ_SCRAP_ACK = b"req_scrap_ack"
REP_SCRAP_ACK_CONN = b"rep_scrap_ack_conn"
REP_SCRAP_ACK_NO_CONN = b"rep_scrap_ack_no_conn"

REP_CLIENT_INFO = b"client_info"
REP_CLIENT_NODE = b"client_node"
