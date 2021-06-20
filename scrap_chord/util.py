from typing import List
from utils.tools import get_id

from utils.search_tree import SearchTree


def in_between(m, key, lwb, upb):
    if lwb <= upb:
        return lwb <= key and key <= upb
    else:
        return (lwb <= key and key <= upb + (2 ** m)) or (
            lwb <= key + (2 ** m) and key <= upb
        )

def parse_requests(command:str):
    args = command.split()
    requests = []
    index = 0
    while index < len(args):
        arg_1 = str(args[index])
        arg_1 = remove_back_slashes(arg_1)
        if arg_1 == "":
            continue

        try:
            arg_2 = int(args[index + 1])
            index += 1
        except (IndexError, ValueError):
            arg_2 = 1
        index += 1
        requests.append((arg_1, arg_2))
    return requests

def select_target_node(url, known_nodes, bits): 
    if len(known_nodes) == 1:
            return (known_nodes[0][0], (known_nodes[0][1][0], known_nodes[0][1][1] + 1))
    url_id = get_id(url, bits)
    lwb_id, _ = known_nodes[-1]
    for node_id, addr in known_nodes:
        if in_between(bits, url_id, lwb_id + 1, node_id):
            return (node_id, (addr[0], addr[1] + 1))
        lwb_id = node_id
    
    raise Exception("A node must be always found")

def reset_times(url, known_nodes, pending_req, value, bits):
    if len(known_nodes) == 0:
        return
    elif len(known_nodes) == 1:
        for urlx in pending_req:
            pending_req[urlx] = value
    else:
        idx, _ = select_target_node(url, known_nodes, bits)
        pending_req[url] = value
        for urlx in pending_req:
            if idx == select_target_node(urlx, known_nodes, bits):
                pending_req[urlx] = value

def remove_back_slashes(url:str) -> str:
    index = len(url) - 1
    while index >= 0 and url[index] == "/":
        index -= 1
    return url[:index + 1]

def add_search_tree(search_trees:list, url:str, depht):
    st = SearchTree(url, depht)
    search_trees.append(st)

def update_search_trees(search_trees:List[SearchTree], url:str, url_set:set) -> set:
    pending = set()
    remove = []
    for i, st in enumerate(search_trees):
        if st.pending_update(url):
            for urlx in st.update(url, url_set):
                pending.add(urlx)
            if st.completed:
                remove.append(i)
    
    remove.reverse()
    completed = [search_trees.pop(i) for i in remove]
    return pending, completed

# a = {"a": 1}
# print(len(a))
# del a["a"]
