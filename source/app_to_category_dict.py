

#Create App Dictionary: Main function
import requests
requests.packages.urllib3.disable_warnings()
from biokbase.catalog.Client import Catalog
from biokbase.narrative_method_store.client import NarrativeMethodStore
catalog = Catalog(url = "https://kbase.us/services/catalog")
nms = NarrativeMethodStore(url = "https://kbase.us/services/narrative_method_store/rpc")
from data_configure import data_configure
from invert_app_dict import dict_invert

import pandas as pd

def create_app_dictionary_2():
    apps = nms.list_methods({"tag": "release"})
    apps_datastruc = pd.DataFrame.from_dict(apps)
    ModDfApps = data_configure(apps_datastruc)
    ModDfApps.drop(['app_type', 'authors', 'git_commit_hash', 'icon', 'input_types', 'module_name', 'name', 'namespace',
                    'output_types', 'subtitle', 'tooltip', 'ver'], axis=1, inplace=True)
    keys = list(set([item for sublist in list(ModDfApps.categories) for item in sublist]))
    app_dict = {k: [] for k in keys}

    for i in ModDfApps.index.values:

        app_category_lst = ModDfApps["categories"][i]
        for category in app_category_lst:
            if category in app_dict.keys():
                app_dict[category].append(ModDfApps["id"][i])
            else:
                raise KeyError("{} not a KBase app category".format(category))

    inverted_dict = dict_invert(app_dict)
    return inverted_dict
