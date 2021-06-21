import sys
from scrap_chord import ScrapChordClient, ScrapChordNode
from scraper import Scraper

def main(node_type, args):
    if node_type in {"chord", "scrapchord", "sc"}:
        port = int(args[0])
        m = int(args[1])
        visible = int(args[2]) if len(args) > 3 else 8
        forever = True if len(args) > 4 and args[3] == 'v' else False
        sc = ScrapChordNode(port, m)
        sc.run(visible=visible, forever=forever)
    
    elif node_type in {"scraper", "scrapper", "s"}:
        port = int(args[0])
        max_t = int(args[1])
        s = Scraper(port, max_t)
        visible = int(args[2]) if len(args) > 3 else 15
        forever = True if len(args) > 4 and args[3] == 'v' else False
        s.run(visible=visible, forever=forever)
    
    elif node_type in {"client", "cli", "c"}:
        port = int(args[0])
        m = int(args[1])
        addr = args[2] if len(args) >= 3 else ""
        c = ScrapChordClient(port, m)
        c.run(addr)


if __name__ == "__main__":
    node_type, *args = sys.argv[1:]
    # try:
    #     
    # except ValueError:
    #     print("DEBUGING")
    #     node_type = "sc"
    #     args = ["7070", "5", "f"]#, "127.0.1.1:7050"]
    main(node_type, args)