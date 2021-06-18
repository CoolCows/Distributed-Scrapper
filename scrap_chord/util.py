from typing import List

from sortedcontainers.sortedset import SortedSet
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
        except (IndexError, ValueError) as exception:
            arg_2 = 1
        index += 1
        requests.append((arg_1, arg_2))
    return requests

def remove_back_slashes(url:str) -> str:
    index = len(url) - 1
    while index >= 0 and url[index] == "/":
        index -= 1
    return url[:index + 1]

def add_to_dict(pending_recv, url_request):
    try:
        pending_recv[url_request] += 1
    except KeyError:
        pending_recv[url_request] = 1

def add_search_tree(search_trees:list, url:str, depht):
    st = SearchTree(url, depht)
    search_trees.append(st)

def update_search_trees(search_trees:List[SearchTree], url:str, url_list:set) -> set:
    pending = set()
    remove = []
    for i, st in enumerate(search_trees):
        if st.pending_update(url):
            for urlx in st.update(url, url_list):
                pending.add(urlx)
            if st.completed:
                remove.append(i)

    for i in range(len(remove)):
        j = len(remove) - 1 - i
        search_trees.pop(j)

    return pending

# a = {"a": 1}
# print(len(a))
# del a["a"]
# print(len(a))

def a(*b):
    print(b)

a(1,2,3)