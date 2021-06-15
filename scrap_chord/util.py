def in_between(m, key, lwb, upb):
    if lwb <= upb:
        return lwb <= key and key <= upb
    else:
        return (lwb <= key and key <= upb + (2 ** m)) or (
            lwb <= key + (2 ** m) and key <= upb
        )
