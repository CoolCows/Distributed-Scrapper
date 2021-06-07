CHORD_PORT = 8000
CHORD_IPROC_PORT = 8001
SCRAP_IPROC_PORT = 8002
BEACON_PORT = 8010

ASK_JOIN = b'qjoin'
ASK_PRED = b'qpred'
ASK_SUCC = b'qsucc'
ASK_STAB = b'qstab'

ANS_JOIN = b'rjoin'
ANS_PRED = b'rpred'
ANS_SUCC = b'rsucc'
ANS_STAB = b'rstab'

ACK = b'ack'
STOP = b'stop'
LEAVE = b'leave'

TIMEOUT_STABILIZE = 1000