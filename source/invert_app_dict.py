
# Invert app dictionary 
# App_Id -> Category
def dict_invert(d):
    inv = {}
    
    for k, v in d.items():
        for item in v:
            if item in inv.keys():
                inv[item].append(k)
            else:
                inv[item] = [k]
    return inv

