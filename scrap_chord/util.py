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
