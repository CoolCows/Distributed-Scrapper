from collections import namedtuple


FunctionCall = namedtuple("FunctionCall", "name params")


def print_finger(node_id, f):
    print(f"Finger Table:{node_id}")
    print("-------------")
    for index, i in enumerate(f):
        info = ""
        if i is not None:
            if index == 0:
                info = "(predecessor)"
                print(f"{info}: {i[0]}")
                continue
            elif index == 1:
                info = "(successor)"
            print(
                f"index:{index}  start:{(node_id + 2**(index -1)) % (2**(len(f)-1)) }  succ:{i[0]} {info}"
            )
    print("-------------")

    