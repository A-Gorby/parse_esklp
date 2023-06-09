import os
import sys
import glob
import logging
import zipfile
import datetime
import pandas as pd
import re

from utils_io import logger
if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io import logger

def find_last_fn_pickle(prefix, path_files):
    fn_pickle = None
    if prefix is None: prefix =''
    # fn_list = sorted(glob.glob(os.path.join(path_files, prefix) + '*.pickle'))
    # fn_list = sorted(glob.glob(path_files + prefix + '*.pickle'))
    fn_list = sorted(glob.glob(os.path.join(path_files, prefix + '*.pickle')))
    # print(fn_list)
    if len(fn_list)>0:  fn_pickle = fn_list[-1]
    return fn_pickle
    
def restore_df_from_pickle(prefix, path_files, fn_pickle):
    # print(f"restore_df_from_pickleЖ prefix: '{prefix}', path_files: '{path_files}', fn_pickle: '{fn_pickle}'")
    if fn_pickle == 'last':
        # fn_pickle = 'smnn_list_v2022_09_23.pickle'
        #smnn_list_df_esklp
        fn_pickle = find_last_fn_pickle(prefix, path_files = path_files)
    elif fn_pickle is not None:
        pass
        # fn_pickle = fn_pickle
    else:
        fn_pickle = find_last_fn_pickle(prefix = prefix, path_files = path_files)
    # print(f"restore_df_from_pickle: fn_pickle: {fn_pickle}")
    if fn_pickle is None:
        logger.error('Restore pickle from ' + path_files + ' failed!')
        sys.exit(2)
    if os.path.exists(os.path.join(path_files, fn_pickle)):
        df = pd.read_pickle(os.path.join(path_files, fn_pickle))
        logger.info('Restore ' + fn_pickle + ' done!')
    else:
        logger.error('Restore ' + fn_pickle + ' from ' + path_files + ' failed!')
    return df
    
def unzip_esklp(path_esklp_source, fn_esklp_xml_active_zip, work_path):
    logger.info('Unzip ' + fn_esklp_xml_active_zip + ' start...')

    try:
        # with zipfile.ZipFile(path_esklp_source + fn_esklp_xml_active_zip, 'r') as zip_ref:
        with zipfile.ZipFile(os.path.join(path_esklp_source, fn_esklp_xml_active_zip), 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logger.info('Unzip ' + fn_esklp_xml_active_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logger.error('Unzip error: ' + str(err))
        sys.exit(2)

def unzip_file(path_source, fn_zip, work_path):
    logger.info('Unzip ' + fn_zip + ' start...')

    try:
        # with zipfile.ZipFile(path_source + fn_zip, 'r') as zip_ref:
        with zipfile.ZipFile(os.path.join(path_source, fn_zip), 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logger.info('Unzip ' + fn_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logger.error('Unzip error: ' + str(err))
        sys.exit(2)

def save_df_to_pickle(df, path_to_save, fn_main):
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn = fn_main + '_' + str_date + '.pickle'
    # df.to_pickle(path_to_save + fn)
    df.to_pickle(os.path.join(path_to_save, fn))
    logger.info(fn + ' saved to ' + path_to_save)
    return fn

#smnn_prefix_active = 'smnn_list_df_esklp_active'
#smnn_prefix_full = 'smnn_list_df_esklp_full'
#klp_prefix_active = 'klp_list_dict_df_esklp_active'
#klp_prefix_full = 'klp_list_dict_df_esklp_full'

def extract_esklp_date(fn, prefix):
    if fn is None: return None
    # m = re.search(r"(?<=esklp_)\d+", fn)
    m = re.search(fr"(?<={prefix}_)\d+", fn)
    if m is not None:
        esklp_date = m.group()
        # print(f"esklp_date: {esklp_date}")
    else: esklp_date = None
    return esklp_date    
