import sys
from scrap_chord import ScrapChordClient, ScrapChordNode
from scraper import Scraper

def main(node_type, args):
    if node_type in {"chord", "scrapchord", "sc"}:
        port = int(args[0])
        m = int(args[1])
        visible = False if len(args) >= 3 and args[2] =="f" else True
        sc = ScrapChordNode(port, m, visible)
        sc.run()
    
    elif node_type in {"scraper", "scrapper", "s"}:
        port = int(args[0])
        max_t = int(args[1])
        visible = False if len(args) >= 3 and args[2] =="f" else True
        s = Scraper(port, max_t, visible)
        s.run()
    
    elif node_type in {"client", "cli", "c"}:
        port = int(args[0])
        m = int(args[1])
        addr = args[2] if len(args) >= 3 else ""
        c = ScrapChordClient(port, m)
        c.run(addr)


if __name__ == "__main__":
    try:
        node_type, *args = sys.argv[1:]
    except ValueError:
        print("DEBUGING")
        node_type = "sc"
        args = ["7050", "5"]#, "127.0.1.1:7050"]
    main(node_type, args)