import sys
import logging
import zipfile
import datetime


def find_last_fn_pickle(prefix, path_files):
    fn_pickle = None
    if prefix is None: prefix =''
    # fn_list = sorted(glob.glob(os.path.join(path_files, prefix) + '*.pickle'))
    fn_list = sorted(glob.glob(path_files + prefix + '*.pickle'))
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
        logging.error('Restore pickle from ' + path_files + ' failed!')
        sys.exit(2)
    if os.path.exists(os.path.join(path_files, fn_pickle)):
        df = pd.read_pickle(os.path.join(path_files, fn_pickle))
        logging.info('Restore ' + fn_pickle + ' done!')
    else:
        logging.error('Restore ' + fn_pickle + ' from ' + path_files + ' failed!')
    return df
    
def unzip_esklp(path_esklp_source, fn_esklp_xml_active_zip, work_path):
    logging.info('Unzip ' + fn_esklp_xml_active_zip + ' start...')

    try:
        with zipfile.ZipFile(path_esklp_source + fn_esklp_xml_active_zip, 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logging.info('Unzip ' + fn_esklp_xml_active_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logging.error('Unzip error: ' + str(err))
        sys.exit(2)

def unzip_file(path_source, fn_zip, work_path):
    logging.info('Unzip ' + fn_zip + ' start...')

    try:
        with zipfile.ZipFile(path_source + fn_zip, 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logging.info('Unzip ' + fn_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logging.error('Unzip error: ' + str(err))
        sys.exit(2)

def save_df_to_pickle(df, path_to_save, fn_main):
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn = fn_main + '_' + str_date + '.pickle'
    df.to_pickle(path_to_save + fn)
    logging.info(fn + ' saved to ' + path_to_save)
    return fn