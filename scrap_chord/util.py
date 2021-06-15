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
