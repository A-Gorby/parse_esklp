import xmlschema
from lxml import etree
from lxml.etree import parse, XMLSchema
from xml_to_dict import XMLtoDict

import numpy as np
import pandas as pd
import os, sys
import math
from tqdm import tqdm
tqdm.pandas()
import logging
import gc
import warnings
warnings.filterwarnings("ignore")
import pickle

from utils_io import logger
if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io import logger

ns = {"ns2":"http://service.rosminzdrav.ru/ESKLP"}

#def etree_to_dict_03(group: _Element)->dict:
def etree_to_dict_03(group, parser):
    ns_tag = '{http://service.rosminzdrav.ru/ESKLP}'
    #parser = XMLtoDict()
    group_string = etree.tostring(group, pretty_print=True, encoding='UTF-8' ).decode().replace('ns2:','').replace(ns_tag, '')
    group_dict = parser.parse(group_string)
    return group_dict

def load_smnn_list_dict(root, rec_limit = np.inf, n_levels = 8 ): 
# 6707 records 7249 2022_09_07
# 7268 records 7249 active 2022_09_23
# 36682 records 7249 full 2022_09_23
#def load_mnn_list(n_levels = 7): # 525 records

    ns = {"ns2":"http://service.rosminzdrav.ru/ESKLP"}
    ns_tag = '{http://service.rosminzdrav.ru/ESKLP}'
    group_list_tag='ns2:group_list'
    group_tag='ns2:group'
    smnn_list_tag = 'ns2:smnn_list'
    klp_list_tag = 'ns2:klp_list'
    smnn_tag = 'ns2:smnn'
    mnn_tag = 'ns2:mnn'
    parser = XMLtoDict()

    pattern_search = '.' + (n_levels+1)*(f"/{group_list_tag}/{group_tag}") + f"/{smnn_list_tag}/{smnn_tag}"
    #pattern_search = f".//*{group_tag}/{smnn_list_tag}/{smnn_tag}"
    pattern_search = f".//*{group_tag}/{smnn_list_tag}/{smnn_tag}"

    lst = root.findall(pattern_search, ns)
    #print(len(lst))
    mnn_list = []
    i = 0
    if len(lst) > 0:
        #for i, g in tqdm(enumerate(lst), ncols=80, ascii=True, desc='Total'):
        #for i, g in tqdm(enumerate(lst), ncols=80, ascii='.oO0', desc='Total'):
        #for g in tqdm(lst, ncols=80, ascii='.oO0', desc='Total'):
        for g in tqdm(lst, ncols=80, desc='SMNN xml extract'):
             
            g_attrib = g.attrib #['UUID']
            mnn = g.find(mnn_tag, ns).text
            g_attrib['mnn'] = mnn
            parent_group = g.getparent().getparent().attrib
            #parent_group = g.getparent().getparent().getparent().attrib

            klp_list = g.find(klp_list_tag, ns)
            if klp_list: g.remove(klp_list)
            g_dict = etree_to_dict_03(g, parser)
            #parent_group = dict(parent_group)
            #parent_group.update(g_dict)
            #g_dict['smnn'].update(dict(parent_group))
            #g_dict.update(parent_group)
            #g_dict.pop('@parent_group')
            g_dict['smnn']['@parent_group_name'] = parent_group['name']
            g_dict['smnn']['@parent_group_UUID'] = parent_group['UUID']
            g_dict['smnn'].pop('@hash')
            
            
            #mnn_list.append(g.attrib)
            mnn_list.append(g_dict['smnn'])
            i += 1
            #mnn_list.append(g_dict)
            #mnn_list.append(parent_group)
            #print(len(g.attrib), g.attrib)
            if i>rec_limit: break
    return mnn_list    

def load_smnn(path_esklp_source, fn_schema, work_path, fn_esklp_xml_active, n_rec=np.inf):

    logger.info('Loading xml scheme ' + fn_esklp_xml_active + '...')
    # schema = etree.XMLSchema(file = path_esklp_source + fn_schema)
    schema = etree.XMLSchema(file = os.path.join(path_esklp_source, fn_schema))
    parser = etree.XMLParser(schema = schema)
    # parser = etree.XMLParser(schema=schema, huge_tree=True)
    logger.info('Init parse xml ' + fn_esklp_xml_active + ' start...')
    # tree = etree.parse(work_path + fn_esklp_xml_active, parser=parser)
    tree = etree.parse(os.path.join(work_path,fn_esklp_xml_active), parser=parser)
    root = tree.getroot()
    del tree
    gc.collect()
    
    logger.info('Init parse xml ' + fn_esklp_xml_active + ' done!')

    logger.info('Extract smnn:  start...')
    #smnn_list = load_smnn_list_dict(root)
    smnn_list = load_smnn_list_dict(root, rec_limit = n_rec)
    #smnn_list = []
    logger.info('Extract smnn: done!')
    logger.info(f"Итого: {len(smnn_list)} записей") # в официальной выгрузке в Excel 7237 18/08/2022 7253 07/09/2022
    gc.collect(); gc.collect()
    return  smnn_list 

def create_smnn_list_df(smnn_list):
    logger.info('Transformation smnn_list to pandas DataFrame')
    smnn_list_df = pd.DataFrame(smnn_list)
    return smnn_list_df

def get_reccursive_name(c_name: str, d: dict, debug=False) -> str:
    lst = []
    if type(d) == dict: 
        if debug: print(d.keys())
        for k in d.keys(): 
            if type(d[k])== dict: 
                if debug: print("if type(d[k])== dict:")
                l1 = get_reccursive_name(c_name + '_' + k, d[k])
                if type(l1) == list:  lst.extend(l1)
                else: lst.append(l1)
            elif type(d[k]) == str: 
                if debug: print("type(d[k]) == str:")
                lst.append(c_name + '_' + k)
            elif type(d[k])== list: 
                if debug: print("if type(d[k])== list:")
                l1 = get_reccursive_name(c_name + '_' + k, d[k][0])
                if type(l1) == list:  lst.extend(l1)
                else: lst.append(l1)
            else: 
                if debug: print("else:")
                lst.append(c_name + '_' + k)
    elif type(d) == str:
        if debug: print("type(d) == str:")
        lst = [c_name]
        
    elif type(d) == list:
        if debug: print("type(d) == list:")
        l1 = get_reccursive_name(c_name, d[0])
        if type(l1) == list:  lst.extend(l1)
        else: lst.append(l1)
    return lst

def get_reccursive_values( d: dict, debug = False): # -> list, elem of list:
    lst = []
    #debug = True
    if type(d) == dict: 
        
        if debug: print(d.keys())
        l1 = len(d.keys()) * [1]
        for i, k in enumerate(d.keys()): 
            if debug: print( k)
            if type(d[k])== dict: 
                if debug: print("type(d[k])== dict")
                l1_1 = get_reccursive_values(d[k])
                if debug: print("l1_1", l1_1)
                if type(l1_1) == list:  
                    l1.extend(l1_1)
                else: l1.append(l1_1)
                if debug: print("l1", l1)
                lst.extend(l1_1)
                if debug: print("lst", lst)
            elif type(d[k]) == list and type(d[k][0]) == dict:
                if debug: print("type(d[k]) == list and type(d[k][0]) == dict")
                num_records = len(d[k])
                num_keys = 0
                l1_1 = num_records * [1]
                if debug: print("l1_1", l1_1)
                for j, dd in enumerate(d[k]):
                    l1_1[j] = get_reccursive_values(dd)

                num_keys = len(dd.keys())
                lst= [ [l1_1[ir][ik]  for ir in range(num_records)] for ik in range(num_keys)]

            else: 
                l1[i] = d[k]
                if debug: print('!!!', len(l1), i)
                lst.append(l1[i])
                
                if debug: print(lst)
    elif type(d) == list and type(d[0]) == dict:
        if debug: print("type(d) == list and type(d[0]) == dict:")
        num_records = len(d)
        num_keys = 0
        l1_1 =  num_records * [1]
        for i, dd in enumerate(d):
            num_keys = len(dd.keys())
            l1_1[i] = num_keys *[1]  # кол-во записей в списке * колво ключей
            for j, v in enumerate(dd.keys()):
                if debug: print(v)
                if type(dd[v])==dict or type(dd[v])==list: 
                    if debug: print("type(dd[v])==dict or type(dd[v])==list: ")
                    l1_2 = get_reccursive_values(dd[v])
                else: 
                    l1_2 = dd[v]
 
                if debug: print("l1_2", l1_2)
                l1_1[i][j] = l1_2
                if debug: print("l1_1", l1_1)
        lst= [ [l1_1[ir][ik]  for ir in range(num_records)] for ik in range(num_keys)]
        if debug: print("lst", lst)
    else: 
        if debug: print("else")
        lst.append(d)
    return lst

def reformat_smnn_list_df(smnn_list, smnn_list_df):
    logger.info('Reformation smnn_list_df')
    cols_to_reformat_01 = ['grls_mnn', 'form', 'grls_lf_list', 'dosage', 'ftg', ] # 'ath',
    cols_to_reformat_name = []
    for c in cols_to_reformat_01:
        ls = get_reccursive_name(c, smnn_list[1][c])
        if type(ls) == list: cols_to_reformat_name.extend(ls)
        else: cols_to_reformat_name.append(ls)
    #print(cols_to_reformat_name)
    
    cols_to_reformat = ['grls_mnn', 'grls_lf_list', 'dosage', 'ath', 'grls_mnn']
    cols_to_rename = ['code', 'mnn', 'form'] #, 'grlfs_value']

    cols_map_to_reformat_name = {'code': 'code_smnn', 'mnn': 'mnn_standard', 'form': 'form_standard',
                                'grls_mnn_mnn_name': "mnn_standard_list",
                                'grls_lf_list_grls_lf_dosage_name':'dosage_name_standard_list' , 
                                'grls_lf_list_grls_lf_lf_name': 'lf_standard_name_list',
                                'ath_ath_name': 'ath_name', 'ath_ath_code' :'ath_code',
                                'dosage_grls_value': 'dosage_standard_value', 
                                'dosage_dosage_unit_name': 'dosage_standard_unit_name', 
                                'dosage_dosage_unit_okei_code': 'dosage_standard_unit_okei_code',
                                'dosage_dosage_unit_okei_name': 'dosage_standard_unit_okei_name',
                                'dosage_dosage_user_okei_code': 'ls_unit_okei_code',
                                'dosage_dosage_user_okei_name': 'ls_unit_okei_name',
                                'dosage_dosage_user_name': 'ls_unit_name',  
                                'dosage_dosage_num': 'dosage_standard_num', 
                                }
    #cols_map_to_reformat_name.keys()
    reorder_cols =['mnn_standard', 'code', 'okpd2', 'form_standard', 
       'dosage_standard_num',
       'dosage_standard_unit_name', 'dosage_standard_unit_okei_code', 'dosage_standard_unit_okei_name', 'dosage_standard_value',
       'ls_unit_name', 'ls_unit_okei_code','ls_unit_okei_name',
        'ftg', 'ath_code', 'ath_name', 
        'is_znvlp', 'is_narcotic',
        'mnn_standard_list', 'lf_standard_name_list',  'dosage_name_standard_list', 
        'date_create', 'date_start', 'date_change', 
        # '@UUID', '@mnn', '@parent_group_name', '@parent_group_UUID', 
        'smnn_replace_list', 'smnn_descendant', 
        ]
    cols_reformated = list(cols_map_to_reformat_name.values())

    for col in cols_to_reformat:
    
        d = smnn_list[1][col]
        new_cols = get_reccursive_name(col, d)
        
        new_cols2 = [cols_map_to_reformat_name[c] for c in new_cols]
        #logging.info("col:" + col + "--> [" + ', '.join(new_cols) + ']\n--> [' + ', '.join(new_cols2) + ']')
        logger.info('col ' + col + ' transform --> [' + ', '.join(new_cols2) + ']')
        smnn_list_df[new_cols2] = smnn_list_df[col].progress_apply(lambda x: pd.Series(get_reccursive_values(x)))
        gc.collect(); gc.collect()
        
    for col in cols_to_rename:
    
        new_col = cols_map_to_reformat_name[col]
        logger.info('col ' + col + ' rename --> ' + new_col)
        # print(f"col: {col} -> {new_col}")
        #smnn_list_df.rename(f"{col}": f"{new_col}", inplace=True)
        smnn_list_df.rename(columns ={ col: new_col}, inplace=True)

    smnn_list_df['dosage_grls_value'] = smnn_list_df['dosage'].\
        progress_apply(lambda x: x.get('grls_value') if x is not None else None)        

    return smnn_list_df

def get_klp_lst(root):
    # 277998 records (18.08.2022) 
    # 280829 (07.09.2022)
    # 284160 records (23.09.2022)
    ns = {"ns2":"http://service.rosminzdrav.ru/ESKLP"}
    ns_tag = '{http://service.rosminzdrav.ru/ESKLP}'
    group_list_tag='ns2:group_list'
    group_tag='ns2:group'
    smnn_list_tag = 'ns2:smnn_list'
    smnn_tag = 'ns2:smnn'
    mnn_tag = 'ns2:mnn'
    klp_list_tag = 'ns2:klp_list'
    klp_tag = 'ns2:klp'
    code_tag = "ns2:code"
    form_tag = "ns2:form"
    dosage__dosage_num__tag = 'ns2:dosage/ns2:dosage_num'
    dosage__dosage_unit__name__tag = 'ns2:dosage/ns2:dosage_unit/ns2:name'
    dosage__dosage_user__name__tag = 'ns2:dosage/ns2:dosage_user/ns2:name'
    dosage__dosage_user__okei_name__tag = 'ns2:dosage/ns2:dosage_user/ns2:okei_name'

    #pattern_search = './/*' + (n_levels+1)*(f"/{group_list_tag}/{group_tag}") + f"/{smnn_list_tag}/{smnn_tag}/{klp_list_tag}/{klp_tag}"
    pattern_search = f".//*/{klp_tag}"

    lst = root.findall(pattern_search, ns)
    return lst

def load_klp_list_dict(lst, rec_start=0, rec_end = np.inf):         
    # 277998 records (18.08.2022) 
    # 280829 (07.09.2022)
    # 284160 records (23.09.2022)
    ns = {"ns2":"http://service.rosminzdrav.ru/ESKLP"}
    ns_tag = '{http://service.rosminzdrav.ru/ESKLP}'
    group_list_tag='ns2:group_list'
    group_tag='ns2:group'
    smnn_list_tag = 'ns2:smnn_list'
    smnn_tag = 'ns2:smnn'
    mnn_tag = 'ns2:mnn'
    klp_list_tag = 'ns2:klp_list'
    klp_tag = 'ns2:klp'
    code_tag = "ns2:code"
    form_tag = "ns2:form"
    dosage__dosage_num__tag = 'ns2:dosage/ns2:dosage_num'
    dosage__dosage_unit__name__tag = 'ns2:dosage/ns2:dosage_unit/ns2:name'
    dosage__dosage_user__name__tag = 'ns2:dosage/ns2:dosage_user/ns2:name'
    dosage__dosage_user__okei_name__tag = 'ns2:dosage/ns2:dosage_user/ns2:okei_name'
    # 07.12.2022
    # dosage_name_standard_list , dosage_standard_value
    
    parser = XMLtoDict()

    #pattern_search = './/*' + (n_levels+1)*(f"/{group_list_tag}/{group_tag}") + f"/{smnn_list_tag}/{smnn_tag}/{klp_list_tag}/{klp_tag}"
    pattern_search = f".//*/{klp_tag}"

    fl_break = False
    klp_list_dict = []
    if len(lst) > 0:
        i = 0
        # for i, g in enumerate(lst):
        for g in tqdm(lst):
            if i< rec_start: continue
            if i>= rec_end: break
            i += 1
            #g_dict = g.attrib #['UUID']
            g_dict = etree_to_dict_03(g, parser)
            mnn_norm_name = g.find('ns2:mnn_norm_name', ns).text
            g_dict['klp']['mnn_norm_name'] = mnn_norm_name
            parent_smnn = g.getparent().getparent()
            if not parent_smnn.tag.replace(ns_tag,'') == smnn_tag.replace('ns2:',''): 
                print(i, 'smnn_tag error') 
                print(parent_smnn.tag.replace(ns_tag,''), smnn_tag)
            parent_smnn_attrib = parent_smnn.attrib            
            mnn = parent_smnn.find(mnn_tag, ns).text # Он после KLp_list - в коцне
            g_dict['klp']['mnn'] = mnn
            smnn_code = parent_smnn.find(code_tag, ns)
            if smnn_code is not None: smnn_code = smnn_code.text
            g_dict['klp']['code_smnn'] = smnn_code
            form_standard = parent_smnn.find(form_tag, ns)
            if form_standard is not None: form_standard = form_standard.text
            g_dict['klp']['form_standard'] = form_standard
            
            dosage__dosage_num = parent_smnn.find(dosage__dosage_num__tag, ns)
            if dosage__dosage_num is not None: dosage__dosage_num = dosage__dosage_num.text
            g_dict['klp']['dosage_num_standard'] = dosage__dosage_num
            dosage__dosage_unit__name = parent_smnn.find(dosage__dosage_unit__name__tag, ns)
            if dosage__dosage_unit__name is not None: dosage__dosage_unit__name = dosage__dosage_unit__name.text
            g_dict['klp']['dosage_unit_name_standard'] = dosage__dosage_unit__name

            dosage__dosage_user__name = parent_smnn.find(dosage__dosage_user__name__tag, ns)
            if dosage__dosage_user__name is not None: dosage__dosage_user__name = dosage__dosage_user__name.text
            g_dict['klp']['lp_unit_name'] = dosage__dosage_user__name

            dosage__dosage_user__okei_name = parent_smnn.find(dosage__dosage_user__okei_name__tag, ns)
            if dosage__dosage_user__okei_name is not None: dosage__dosage_user__okei_name = dosage__dosage_user__okei_name.text
            g_dict['klp']['lp_unit_okei_name'] = dosage__dosage_user__okei_name

            #g_dict['klp']['parent_smnn_UUID'] = parent_smnn_attrib['UUID']
            # лишнее
            #g_dict.update(dict(parent_smnn_group_attrib))
            g_dict['klp'].pop('@hash')
            
            klp_list_dict.append(g_dict['klp'])
            
            if fl_break: break
            #if i>rec_limit: break
    del lst
    gc.collect(); gc.collect()
    return klp_list_dict

    # smnn_list = load_smnn(path_esklp_source, fn_schema, work_path, 
                # fn_esklp_xml_active) #, n_rec=100)
def load_klp_list(path_esklp_source, fn_schema, work_path, 
            fn_esklp_xml_active, n_rec=np.inf):
    logger.info('Loading xml scheme ' + fn_esklp_xml_active + '...')
    # schema = etree.XMLSchema(file = path_esklp_source + fn_schema)
    schema = etree.XMLSchema(file = os.path.join(path_esklp_source, fn_schema))
    
    parser = etree.XMLParser(schema = schema)
    # parser = etree.XMLParser(schema=schema, huge_tree=True)
    logger.info('Init parse xml ' + fn_esklp_xml_active + ' start...')
    # tree = etree.parse(work_path + fn_esklp_xml_active, parser=parser)
    tree = etree.parse(os.path.join(work_path, fn_esklp_xml_active), parser=parser)
    root = tree.getroot()
    del tree
    gc.collect()
    logger.info('Init parse xml ' + fn_esklp_xml_active + ' done!')

    logger.info('Extract klp:  start...')
    #smnn_list = load_smnn_list_dict(root)
    klp_lst = get_klp_lst(root)
    klp_list_dict = load_klp_list_dict(klp_lst, rec_start=0, rec_end = n_rec)
    logger.info('Extract klp: done!')
    logger.info(f"Итого: {len(klp_list_dict)} записей") 
    gc.collect()
    return  klp_list_dict 

def get_reccursive_name_supp(c_name: str, d: dict, debug=False):
    lst = []
    if type(d) == dict: 
        #lst = []    
        if debug: print(d.keys())
        for k in d.keys(): 
            #print(k)
            if type(d[k])== dict: 
                if debug: print("if type(d[k])== dict:")
                l1 = get_reccursive_name_supp(c_name + '/' + k, d[k], debug)
                if type(l1) == list:  lst.extend(l1)
                else: lst.append(l1)
            elif type(d[k]) == str: 
                if debug: print("type(d[k]) == str:")
                lst.append(c_name + '/' + k)
            elif type(d[k])== list: 
                if debug: print("if type(d[k])== list:")
                l1=[]
                for dd in d[k]:
                    l1 =  l1 + get_reccursive_name_supp(c_name + '/' + k, dd, debug)
                l1= list(set(l1))
                if type(l1) == list:  lst.extend(l1)
                else: lst.append(l1)
            else: 
                if debug: print("else:")
                lst.append(c_name + '/' + k)
    elif type(d) == str:
        if debug: print("type(d) == str:")
        lst = [c_name]
        #lst = c_name
    elif type(d) == list:
        if debug: print("type(d) == list:")
        l1 = get_reccursive_name_supp(c_name, d[0], debug)
        if type(l1) == list:  lst.extend(l1)
        else: lst.append(l1)
    return lst

def get_reccursive_name_02(c_name: str, d: dict, debug=False):
    lst_str = get_reccursive_name_supp(c_name, d, debug)
    #print(lst)
    lst = ['_'.join(l.split('/')) for l in lst_str]
    return lst, lst_str

def get_reccursive_values_supp( d, c_names_str, level, None_value = None, debug=False):
    lst = []
    
    
    if type(d) == dict: 
        #lst = []    
        try: 
            level_keys = [tag.split('/')[level+1] for tag in  c_names_str]
            if debug: print(level, ': level_keys:', level_keys )
        except Exception as err:
            level_keys = d.keys()
            if debug: print("if type(d) == dict:"); print("-->", d.keys())
        #for k in d.keys(): 
        for k in level_keys: 
            #print(k)
            #if type(d[k])== dict: 
            if d.get(k) is not None and type(d[k])== dict: 
                if debug: print("if type(d[k])== dict:"); print("-->", d[k])
                l1 = get_reccursive_values_supp(d[k], c_names_str, level+1, None_value=None_value, debug=debug)
                if type(l1) == list:  lst =l1
                else: lst = [l1]
                #if type(l1) == list:  lst.extend(l1)
                #else: lst = l1
                if debug: print(level, lst)
                #else: lst.append(l1)
                #lst.append(l1)
            elif d.get(k) is not None and type(d[k])== list: 
                if debug: print("if type(d[k])== list:"); print("-->", d[k])
                l1 = get_reccursive_values_supp(d[k], c_names_str, level, None_value=None_value, debug=debug)
                if type(l1) == list:  lst =l1
                else: lst = [l1]
                #if type(l1) == list:  lst.extend(l1)
                #else: lst = l1
                #else: lst.append(l1)
                #else: lst = l1
                #lst.append(l1)
                if debug: print(level, lst)
            elif d.get(k) is not None and type(d[k]) == str: 
                if debug: print("type(d[k]) == str:"); print("-->", d[k])
                lst.append(d[k])
                if debug: print(level, lst)
            elif d.get(k) is not None: 
                if debug: print("else:")
                lst.append(d[k])
                if debug: print(level, lst)
            elif d.get(k) is None:  
                lst.append(None_value)
                if debug: print(level, lst)

            #else: 
            #    if debug: print("else:")
            #    lst.append(d[k])
    elif type(d) == list:
        if debug: print("type(d) == list:"); print(d)
        #l1 = get_reccursive_values(d[0])
        #if type(l1) == list:  lst.extend(l1)
        #else: lst.append(l1)
        if len(d) > 0 and type(d[0])==dict:
            if debug: print("if len(d) > 0 and type(d[0])==dict:"); print("-->", d[0])
            l1=[]
            for ii, it in enumerate(d):
                l2 = get_reccursive_values_supp( it, c_names_str, level+1, None_value=None_value, debug=debug)
                l1.append(l2)
            #if type(l2) == list:  l1.extend(l1)
            #else: lst.append(l1)
            #lst.append(l1)
            lst = l1
            if debug: print("lst:", lst)
        else: 
            if debug: print("else:"); print("-->", d)
            #lst.append(d)
            lst = d
    elif type(d) == str:
        if debug: print("type(d) == str:"); print("-->", d)
        lst = d
        #lst = c_name
    else: 
        if debug: print("else"); print("-->", d)
        lst = d

    return lst
def get_reccursive_values_02( d, c_names_str, None_value=None, debug=False):    
    lst = get_reccursive_values_supp( d, c_names_str, level=0, None_value=None_value, debug=debug)
    #print("rezult_1:", lst)
    if type(lst) ==list and len(lst)>0 and type(lst[0])==list and not type(lst[0])==str: 
        #print(" if type(lst) ==list")
        #print(type(lst[0]))
        
        lst = list(map(list, zip(*lst)))    
        # https://stackoverflow.com/questions/6473679/transpose-list-of-lists
    
    return lst
    
def create_klp_list_dict_df(klp_list_dict):
    logging.info('Transformation klp_list_dict to pandas DataFrame')
    klp_list_dict_df = pd.DataFrame(klp_list_dict)
    return klp_list_dict_df

def find_rec_lp_date_deactivate(klp_list_dict_df):
    b, e = 0, np.inf
    for i, row in klp_list_dict_df[klp_list_dict_df['klp_lim_price_list'].notnull()].iterrows(): 
        if i < b: continue
        if i > e: break
        d = row['klp_lim_price_list']
        #print(type(d))
        if type(d) ==dict:
            if type(d['klp_lim_price'])==list:
                for dd in d['klp_lim_price']:
                    #print(dd.keys())
                    if dd.get('date_deactivate') is not None:
                        print("find_rec_lp_date_deactivate:", i, d)
                        return i
                        # break
                        # break
    # если не находит возвращает последнее зачение
    print("find_rec_lp_date_deactivate - not finded, but i by notnull klp_LP:", i, d)
    return i

def find_rec_mass_volume(klp_list_dict_df):
    b, e = 0, np.inf
    for i, row in klp_list_dict_df[klp_list_dict_df['mass_volume'].notnull()].iterrows(): 
        if i < b: continue
        if i > e: break
        d = row['mass_volume']
        if d is not None :
            print("find_rec_mass_volume: ", i, d)
            break
        
    return i

def klp_lim_price_list_extract(c):
    klp_new_cols = ['lim_price_value', 'lim_price_reg_date',  'lim_price_reg_num', 'lim_price_barcode', 'lim_price_date_deactivate']
    klp_exist_cols = ['price_value', 'reg_date', 'reg_num', 'barcode', 'date_deactivate']
    if c is not None:
        elem = c['klp_lim_price']
        if type(elem)==list:
            if len(elem)>0:
                lst_return = np.empty( (len(klp_new_cols), len(elem)), dtype=object)
                for i_r, d_l in enumerate(elem):
                    for i_c, col in enumerate(klp_exist_cols):
                        lst_return[i_c, i_r] = d_l.get(col)
        elif type(elem)==dict:
            lst_return = np.empty( (len(klp_new_cols)), dtype=object)
            for i_c, col in enumerate(klp_exist_cols):
                lst_return[i_c] = elem.get(col)
        else:
            print ("type(elem)!=list & != dict", c) #, lst)
            lst_return = len(klp_new_cols)* [None]
    else:
        lst_return = len(klp_new_cols)* [None]
    price_value, reg_date, reg_num, barcode, date_deactivate = lst_return
    return price_value, reg_date, reg_num, barcode, date_deactivate

def load_form_standard_unify_dict(data_esklp_supp_dicts_dir):
    fn_dict = "form_standard_unify_dict.pickle"
    # path_supp_dicts = 'D:/DPP/01_parsing/data/supp_dicts/'
    path_supp_dicts = data_esklp_supp_dicts_dir
    # with open(path_supp_dicts + fn_dict, 'rb') as f:
    with open(os.path.join(path_supp_dicts, fn_dict), 'rb') as f:
        form_standard_unify_dict = pickle.load(f)
    return form_standard_unify_dict

def reformat_klp_list_dict_df(klp_list_dict, klp_list_dict_df, smnn_list_df, data_esklp_supp_dicts_dir):
    
    reformatted_cols = ['klp_lim_price_list_klp_lim_price_reg_num', 'klp_lim_price_list_klp_lim_price_reg_date', 'klp_lim_price_list_klp_lim_price_price_value', 
    'klp_lim_price_list_klp_lim_price_barcode', 'klp_lim_price_list_klp_lim_price_date_deactivate']
    reformat_col_new_name = ['lim_price_reg_num', 'lim_price_reg_date', 'lim_price_value',  'lim_price_barcode', 'lim_price_date_deactivate']
    n_cols_auto = ['pack_1_num', 'pack_1_name', 'pack_2_num', 'pack_2_name', 
    'owner_name', 'owner_country_code', 'owner_country_name', 
    'manufacturer_name', 'manufacturer_country_code', 'manufacturer_country_name', 'manufacturer_address', 
    'mass_volume_num', 'mass_volume_name',
    ]
    n_cols_map = {v: v for v in n_cols_auto}
    n_cols_map.update({i1:i2 for i1, i2 in zip(reformatted_cols,reformat_col_new_name )})

    cols_to_reformat_klp = [ 'klp_lim_price_list', 'pack_1', 'pack_2', 
        'owner', 'manufacturer', 'mass_volume',]
    reorder_cols_klp = ['code_smnn','mnn_standard', 'form_standard', 'is_dosed','dosage_num_standard', 'dosage_unit_name_standard',
        'code_klp', 'trade_name', 'mnn_norm_name', 'lf_norm_name' , 'dosage_norm_name',
        'lp_unit_name', 'lp_unit_okei_name',
        'consumer_total', # 'pack_1_num' * 'pack_2_num'
        'pack_1_num', 'pack_1_name', 'pack_2_num', 'pack_2_name',
        'completeness',
        'mass_volume_num', 'mass_volume_name',
        'num_reg', 'owner_name', 'owner_country_code', 'owner_country_name', 'date_reg',
        'manufacturer_name', 'manufacturer_country_code', 'manufacturer_country_name', 'manufacturer_address',
        'is_znvlp', 'is_narcotic',
        'date_create', 'date_start', 'date_change', 
        #'klp_lim_price_list', 
        'lim_price_reg_num', 'lim_price_reg_date', 'lim_price_value',  'lim_price_barcode', 'lim_price_date_deactivate'
        'klp_replace_list','klp_descendant_list',
        ]
    cols_to_rename_klp = ['mnn', 'code'] 

    cols_map_to_reformat_name_klp = {"mnn":"mnn_standard", 'code': "code_klp",}
    cols_map_to_reformat_name_klp.update(n_cols_map)
    # print(cols_map_to_reformat_name_klp.keys())
    cols_reformated_klp = list(cols_map_to_reformat_name_klp.values())
    reсord_ind_lp = find_rec_lp_date_deactivate(klp_list_dict_df)
    # не находит date_deactivate в ЕСКЛП от 10.11.2022
    reсord_ind_mv = find_rec_mass_volume(klp_list_dict_df)
    
    for col in cols_to_reformat_klp:
    # for col in cols_to_reformat_klp[1:]: # exclude 'klp_lim_price_list'
    #for col in cols_to_reformat_klp[-1:]: # mass_volume    Wall time: 1min 14s v 2022_09_07
        # if col in ['mass_volume']:
        if col == 'mass_volume':
            reсord_ind = reсord_ind_mv
        else:
            reсord_ind = reсord_ind_lp
        d = klp_list_dict[reсord_ind][col]
        # new_cols = get_reccursive_name(col, d)
        new_cols, c_names_str = get_reccursive_name_02(col, d)
        # print(col, new_cols)
        new_cols2 = [cols_map_to_reformat_name_klp[c] for c in new_cols]
        
        # print("col:", col, "-->", new_cols, '\n-->', new_cols2)
        
        if col == 'klp_lim_price_list':
            # автоматом выдает:
            # [lim_price_date_deactivate, lim_price_barcode, lim_price_reg_num, lim_price_reg_date, lim_price_value]
            # new_cols2 = ['lim_price_reg_date', 'lim_price_reg_num', 'lim_price_barcode', 'lim_price_date_deactivate', 'lim_price_value']
            # new_cols2 = ['lim_price_date_deactivate', 'lim_price_barcode', 'lim_price_reg_num', 'lim_price_reg_date', 'lim_price_value']
            klp_new_cols = ['lim_price_value', 'lim_price_reg_date',  'lim_price_reg_num', 'lim_price_barcode', 'lim_price_date_deactivate']
            klp_exist_cols = ['price_value', 'reg_date', 'reg_num', 'barcode', 'date_deactivate']
            new_cols2 = ['lim_price_value', 'lim_price_reg_date',  'lim_price_reg_num', 'lim_price_barcode', 'lim_price_date_deactivate']
            logger.info("col '" + col + "' transform --> [" + ', '.join(new_cols2) + ']')
            klp_list_dict_df[new_cols2] = None
            mask_w = klp_list_dict_df[col].notnull()
            klp_list_dict_df.loc[mask_w, klp_new_cols] = klp_list_dict_df.loc[mask_w, 'klp_lim_price_list'].\
                progress_apply(lambda x: pd.Series(klp_lim_price_list_extract(x), index=new_cols2))

            #  klp_list_dict_df.loc[mask_w, new_cols2] = klp_list_dict_df.loc[mask_w, col].progress_apply(\
            #   lambda x : get_reccursive_values_02(x, c_names_str, None_value='', debug=False) ) 
            #   #lambda x :pd.Series(get_reccursive_values_02(x, c_names_str, None_value='', debug=False)))
            # for i, row  in tqdm(klp_list_dict_df[mask_w].iterrows()):
            #     klp_list_dict_df.loc[i, new_cols2] = pd.Series(klp_lim_price_list_extract(row[col]))

                # try: 
                #     rez_list = get_reccursive_values_02(row[col], c_names_str, None_value='', debug=False)
                #     if len(rez_list) < len(new_cols2): 
                #         # не всегда LP_date_deactivate в колонке и в dict
                #         # rez_list.extend( (len(new_cols2) - len(rez_list))*[None])
                #         rez_list.insert(0, None)
                #     klp_list_dict_df.loc[i, new_cols2] = rez_list
                # except Exception as err:
                #     print(i, err)
                
        else:
            logger.info("col '" + col + "' transform --> [" + ', '.join(new_cols2) + ']')
            klp_list_dict_df[new_cols2] = klp_list_dict_df[col].progress_apply(lambda x: pd.Series(get_reccursive_values(x)))
        gc.collect(); gc.collect()

    for col in cols_to_rename_klp:
    
        new_col = cols_map_to_reformat_name_klp[col]
        logger.info("Rename col '" + col + "' --> '" + new_col + "'")
        # print(f"col: {col} -> {new_col}")
        klp_list_dict_df.rename(columns = {col: new_col}, inplace=True)

    logger.info("Extend form_standard_unify: main start...")
    form_standard_unify_dict = load_form_standard_unify_dict(data_esklp_supp_dicts_dir)
    form_standard_unify_dict_reverse = {vv: k for k,v in form_standard_unify_dict.items() for vv in v}
    
    f_s_u_d = form_standard_unify_dict_reverse.keys()
    f_s_u_k = klp_list_dict_df['form_standard'].unique()
    f_s_list_to_add = list(set(f_s_u_k).difference(set(f_s_u_d)))
    if len(f_s_list_to_add)> 0: 
        logger.info(f"Need to extend form_standard_unify by new_values KLP:\n" + '\n'.join(f_s_list_to_add))

    klp_list_dict_df['form_standard_unify'] = klp_list_dict_df['form_standard'].\
        progress_apply(lambda x: form_standard_unify_dict_reverse.get(x))
    
    form_standard_unify_dict_reverse_02 = {'НАБОР КАПСУЛ С ПОРОШКОМ ДЛЯ ИНГАЛЯЦИЙ': 'Капсулы',
            'НАБОР ТАБЛЕТОК, ПОКРЫТЫХ ОБОЛОЧКОЙ' : 'Таблетки'}

    def update_pharm_form_unify_02(x):
        if x in ['НАБОР КАПСУЛ С ПОРОШКОМ ДЛЯ ИНГАЛЯЦИЙ', 'НАБОР ТАБЛЕТОК, ПОКРЫТЫХ ОБОЛОЧКОЙ']:
            return form_standard_unify_dict_reverse_02.get(x)
        else:
            return form_standard_unify_dict_reverse.get(x)
    logger.info("Extend 'form_standard_unify': update Sets...")
    klp_list_dict_df['form_standard_unify'] = klp_list_dict_df['form_standard'].\
        progress_apply(lambda x: update_pharm_form_unify_02(x) )
    logger.info("Extend 'form_standard_unify': done!")
    gc.collect(); gc.collect()

    # form_standard_unify_dict.values

    logger.info("Extend 'lim_price_reg_date_str'")
    # klp_list_dict_df['lim_price_reg_date_str'] = klp_list_dict_df['lim_price_reg_date'].\
    #     progress_apply(lambda x: ';'.join(x) if x is not None and type(x)==list else ('' if x is None else x)) 
    klp_list_dict_df['lim_price_reg_date_str'] = klp_list_dict_df['lim_price_reg_date'].\
        progress_apply(lambda x: ';'.join(x) if x is not None and type(x)==list else ('' if x is None else x))       
    logger.info(f"lim_price_reg_date_str.shape: " +
    f"{klp_list_dict_df[klp_list_dict_df['lim_price_reg_date_str'].notnull() & (klp_list_dict_df['lim_price_reg_date_str'].str.len()>2)].shape[0]}")
    gc.collect(); gc.collect()

    logger.info("Extend 'lim_price_barcode_str'")
    klp_list_dict_df['lim_price_barcode_str'] = klp_list_dict_df['lim_price_barcode'].\
        progress_apply(\
      lambda x: ';'.join(x) if x is not None and type(x)==list else ('' if x is None else x))
    logger.info(f"lim_price_barcode_str.shape: " + 
    f"{klp_list_dict_df[klp_list_dict_df['lim_price_barcode_str'].notnull() & (klp_list_dict_df['lim_price_barcode_str'].str.len()>2)].shape[0]}")
    gc.collect(); gc.collect()

    logger.info("Extend klp: + 'dosage_name_standard_list' , 'dosage_standard_value' - start...")
    smnn_cols = ['dosage_name_standard_list' , 'dosage_standard_value']
    new_cols = ['dosage_standard_value_str_list_klp' , 'dosage_standard_value_str_klp']
    # klp_list_dict_df.loc[:3, new_cols] = klp_list_dict_df.loc[:3,'code_smnn'].progress_apply(lambda x: pd.Series(smnn_list_df[smnn_list_df['code_smnn']==x][smnn_cols].values[0], index=new_cols))
    # v1 - оч медленый
    # klp_list_dict_df[new_cols] = klp_list_dict_df['code_smnn'].progress_apply(lambda x: \
    #     pd.Series(smnn_list_df[smnn_list_df['code_smnn']==x][smnn_cols].values[0], index=new_cols))
    # v1 - оч быстрый
    # klp_list_dict_df = klp_list_dict_df.merge(smnn_list_df, how='left', on='code_smnn', suffixes=(None,'_y')) 
    klp_list_dict_df = klp_list_dict_df.merge(smnn_list_df[['code_smnn'] + smnn_cols], how='left', on='code_smnn', suffixes=(None,'_y')) 

    # klp_tmp_cols = klp_list_dict_df.columns
    # klp_tmp_cols_y = [c for c in klp_tmp_cols if c.endswith('_y')]
    # # klp_tmp_cols_y, smnn_columns_cut
    # smnn_columns_cut = ['okpd2',   'code_smnn',
    # # 'mnn_standard',
    # 'grls_mnn',
    # # 'form_standard',
    # 'grls_lf_list',  'dosage',  'ftg',  'ath',
    # # 'is_znvlp',
    # # 'is_narcotic',
    # # 'date_create',
    # # 'date_start',
    # # 'date_change',
    # # '@UUID',
    # '@mnn',  '@parent_group_name',  '@parent_group_UUID',  'smnn_replace_list',  'smnn_descendant',  'mnn_standard_list',
    # 'lf_standard_name_list',  'dosage_standard_unit_name',  'dosage_standard_unit_okei_code',  'dosage_standard_unit_okei_name',
    # 'dosage_standard_num',  'ls_unit_name',  'ls_unit_okei_code',  'ls_unit_okei_name',  'ath_name',
    # 'ath_code',  'dosage_grls_value']

    klp_list_dict_df.rename(columns = {'dosage_name_standard_list' : 'dosage_standard_value_str_list_klp', 
                                   'dosage_standard_value': 'dosage_standard_value_str_klp'}, inplace=True)

    logger.info("Extend klp: + 'dosage_name_standard_list' , 'dosage_standard_value' - end")
    logger.info("Reformat klp - done!:"  + str(klp_list_dict_df.shape))

    return klp_list_dict_df

    
