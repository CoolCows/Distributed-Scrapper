CHORD_PORT = 8000
BEACON_PORT = 8010
CHORD_SCRAP_ADDR = "ipc://./ipc/chord-scrapper"

# FLAGS!
# Stablish initial communications between chord node and scrapper
REQ_IPC = b"req_ipc"
REP_IPC = b"rep_ipc"

# Ask to scrap certain url ...
REQ_SCRAP = b"req_scrap"
REP_SCRAP = b"rep_scrap"