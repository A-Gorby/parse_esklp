import pandas as pd
import numpy as np
import os, sys
import re
import xlrd

import json
import itertools
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
from pprint import pprint
import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

from xml_to_dict import XMLtoDict
###import xml.etree.cElementTree as ET
import xmlschema
import xml.etree.ElementTree as ET
from lxml import etree
from lxml.etree import parse, XMLSchema
#from lxml.etree import _Element as Element
from lxml.etree import _Element

from bs4 import BeautifulSoup as bs
# import parsel
#from urllib.request import urlopen
#import requests

import logging
import zipfile
import warnings

from utils_esklp import unzip_esklp, save_df_to_pickle
from xml_utils import load_smnn, create_smnn_list_df, reformat_smnn_list_df
from xml_utils import load_klp_list, create_klp_list_dict_df, reformat_klp_list_dict_df

smnn_prefix_active = 'smnn_list_df_esklp_active_'
smnn_prefix_full = 'smnn_list_df_esklp_full_'
klp_prefix_active = 'klp_list_dict_df_esklp_active_'
klp_prefix_full = 'klp_list_dict_df_esklp_full_'

def parse_esklp (
    fn_esklp_xml_schema, fn_esklp_xml_zip, 
    path_esklp_source, path_esklp_processed,  data_esklp_supp_dicts_dir, data_tmp_dir,
                 fulfillment = 'a'):
    
    if fn_esklp_xml_zip is None:
        logger.error(f"Укажите правильно название xml.zip -файла с ЕСКЛП")
        sys.exit(2)

    if not os.path.exists(os.path.join(path_esklp_source, fn_esklp_xml_zip)):
        logging.error(f"File '{fn_esklp_xml_zip}' not exists in {path_esklp_source} ")
        sys.exit(2)

    # fn_esklp_xml = unzip_esklp(path_esklp_source, fn_esklp_xml_zip, work_path)
    fn_esklp_xml = unzip_esklp(path_esklp_source, fn_esklp_xml_zip, data_tmp_dir)
    # #print(fn_esklp_xml_active)
    if fulfillment == 'a':
        smnn_prefix = smnn_prefix_active
        klp_prefix = klp_prefix_active
    else:
        smnn_prefix = smnn_prefix_full
        klp_prefix = klp_prefix_full

    gc.collect(); gc.collect()
    smnn_list = load_smnn(path_esklp_source, fn_esklp_xml_schema, data_tmp_dir, fn_esklp_xml) #, n_rec=100)
    smnn_list_df = create_smnn_list_df(smnn_list)
    smnn_list_df = reformat_smnn_list_df(smnn_list, smnn_list_df)
    fn_smnn_list_pickle_main = smnn_prefix + fn_esklp_xml.split('_')[1]
    fn_smnn_list_pickle = save_df_to_pickle(smnn_list_df, path_esklp_processed, fn_smnn_list_pickle_main)
    
    # print(fn_smnn_list_pickle)
    gc.collect(); gc.collect()
    klp_list_dict = load_klp_list(path_esklp_source, fn_esklp_xml_schema, data_tmp_dir, fn_esklp_xml) #, n_rec=1000)
    klp_list_dict_df = create_klp_list_dict_df(klp_list_dict)
    klp_list_dict_df, f_s_list_to_add = reformat_klp_list_dict_df(klp_list_dict, klp_list_dict_df, smnn_list_df, data_esklp_supp_dicts_dir)
    fn_klp_list_dict_df_pickle_main = klp_prefix + fn_esklp_xml.split('_')[1]
    fn_klp_list_dict_df_pickle = save_df_to_pickle(klp_list_dict_df, 
         path_esklp_processed, fn_klp_list_dict_df_pickle_main)

    del smnn_list, klp_list_dict
    gc.collect(); gc.collect()
    return fn_smnn_list_pickle, fn_klp_list_dict_df_pickle, f_s_list_to_add
