

#Create App Dictionary: Main function
import requests
requests.packages.urllib3.disable_warnings()
from biokbase.catalog.Client import Catalog
from biokbase.narrative_method_store.client import NarrativeMethodStore
catalog = Catalog(url = "https://kbase.us/services/catalog")
nms = NarrativeMethodStore(url = "https://kbase.us/services/narrative_method_store/rpc")
from data_configure import data_configure

import pandas as pd

def create_app_dictionary_1():
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
                app_dict[category] = list(set(app_dict[category]))
            else:
                raise KeyError("{} not a KBase app category".format(category))

    return app_dict
