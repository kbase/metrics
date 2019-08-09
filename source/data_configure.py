

# Configure App Data: Function

def data_configure(app_df):
    category_mess = list(app_df.categories)
    filters = ["inactive", "viewers"]
    my_idx_list, categories, app_ids = [], [], []

    for idx, lst in enumerate(category_mess):
        if any([True for e in lst if e in filters]):
            my_idx_list.append(idx)
        else:
            lst = [x for x in lst if "active" != x]
            if lst:
                categories.append(lst)
            else:
                my_idx_list.append(idx)

    modDF = app_df.drop(my_idx_list)
    modDF.categories = categories

    return modDF
