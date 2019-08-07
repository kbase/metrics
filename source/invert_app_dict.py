

# Invert app dictionary
# App_Id -> Category
def dict_invert(d):
    inv = {}

    for k, v in d.iteritems():
        for item in v:
            inv[item] = k
    return inv
