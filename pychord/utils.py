from collections import namedtuple


FunctionCall = namedtuple("FunctionCall", "name params")


def in_between(m, key, lwb, lequal, upb, requal):
    if not (lequal or requal) and lwb == ((upb - 1) % 2 ** m):
        return False

    if not lequal:
        lwb = (lwb + 1) % (2 ** m)
    if not requal:
        upb = (upb - 1) % (2 ** m)

    if lwb <= upb:
        return lwb <= key and key <= upb
    else:
        return (lwb <= key and key <= upb + (2 ** m)) or (
            lwb <= key + (2 ** m) and key <= upb
        )


def finger_table_to_str(node_id, finger_table):
    ret = f"Finger Table:{node_id}\n"
    ret += "-------------\n"
    for index, i in enumerate(finger_table):
        info = ""
        if i is not None:
            if index == 0:
                ret += f"predecessor : {i[0]}\n"
                continue
            elif index == 1:
                info = "(successor)"
            start = (node_id + 2 ** (index - 1)) % (2 ** (len(finger_table) - 1))
            ret += f"index:{index}  start:{start}  succ:{i[0]} {info}\n"
    ret += "-------------\n"
    return ret

