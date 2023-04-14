import ipywidgets as widgets
from ipywidgets import Layout, Box, Label
def form_param_esklp(fn_list):
    fn_esklp_xml_zip_file_drop_douwn = widgets.Dropdown( options=fn_list, value=None) #fn_list[0] if len(fn_list) > 0 else None, disabled=False)
    # fn_desc_file_drop_douwn = widgets.Dropdown( options=fn_list, value=None)
    # sheet_name_drop_douwn = widgets.Dropdown( options= [None], value= None, disabled=False)
    # col_name_drop_douwn = widgets.Dropdown( options= [None], value= None, disabled=False)
    # fn_dict_file_drop_douwn = widgets.Dropdown( options= [None] + fn_list, value= None, disabled=False, )
    # radio_btn_big_dict = widgets.RadioButtons(options=['Р”Р°', 'РќРµС‚'], value= 'Р”Р°', disabled=False) # description='Check me',    , indent=False
    # radio_btn_prod_options = widgets.RadioButtons(options=['Р”Р°', 'РќРµС‚'], value= 'РќРµС‚', disabled=False if radio_btn_big_dict.value=='Р”Р°' else True )
    # similarity_threshold_slider = widgets.IntSlider(min=1,max=100, value=90)
    # max_entries_slider = widgets.IntSlider(min=1,max=5, value=4)
    # max_out_values_slider = widgets.IntSlider(min=1,max=10, value=4)

    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите xml.zip-файл со справочником ЕСКЛП:"), fn_esklp_xml_zip_file_drop_douwn], layout=form_item_layout) 
    # desc_box = Box([Label(value='Выберите Excel-файл с описанием моделей:'), fn_desc_file_drop_douwn], layout=form_item_layout) 
    # sheet_box = Box([Label(value='Выберите лист Excel-файла:'), sheet_name_drop_douwn], layout=form_item_layout) 
    # column_box = Box([Label(value='Р—Р°РіРѕР»РѕРІРѕРє РєРѕР»РѕРЅРєРё:'), col_name_drop_douwn], layout=form_item_layout) 
    # big_dict_box = Box([Label(value='РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Р±РѕР»СЊС€РёРµ СЃРїСЂР°РІРѕС‡РЅРёРєРё:'), radio_btn_big_dict], layout=form_item_layout) 
    # prod_options_box = Box([Label(value='РСЃРєР°С‚СЊ РІ Р’Р°СЂРёР°РЅС‚Р°С… РёСЃРїРѕР»РЅРµРЅРёСЏ (+10 РјРёРЅ):'), radio_btn_prod_options], layout=form_item_layout) 
    # similarity_threshold_box = Box([Label(value='РњРёРЅРёРјР°Р»СЊРЅС‹Р№ % СЃС…РѕРґСЃС‚РІР° РїРѕР·РёС†РёР№:'), similarity_threshold_slider], layout=form_item_layout) 
    # max_entries_box = Box([Label(value='РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»-РІРѕ РЅР°Р№РґРµРЅРЅС‹С… РїРѕР·РёС†РёР№:'), max_entries_slider], layout=form_item_layout) 
    # max_out_values_box = Box([Label(value='РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»-РІРѕ РІС‹РІРѕРґРёРјС‹С… РїРѕР·РёС†РёР№:'), max_out_values_slider], layout=form_item_layout) 
    
    # form_items = [check_box, dict_box, big_dict_box, prod_options_box, similarity_threshold_box, max_entries_box]
    form_items = [check_box] #, column_box, similarity_threshold_box, max_entries_box, max_out_values_box]
    
    form_esklp = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_esklp, fn_esklp_xml_zip_file_drop_douwn

def def_form_esklp_upd_unify_pharm_forms(pharm_forms_to_add, unify_pharm_forms):
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    
    unify_pharm_forms_drop_douwn = widgets.Dropdown( options=unify_pharm_forms, value=None)
    # check_box = Box([Label(value="Выберите xml.zip-файл со справочником ЕСКЛП:"), fn_esklp_xml_zip_file_drop_douwn], layout=form_item_layout) 
    # check_box = Box([Label(value=pharm_forms_to_add[0]), unify_pharm_forms_drop_douwn], layout=form_item_layout)
    
    pre_value = [widgets.Dropdown( options=unify_pharm_forms, value=pharm_form.split()[0].capitalize() 
              if (pharm_form.split()[0].capitalize() in unify_pharm_forms) and not (pharm_form.split()[0].capitalize()=='Набор') else None) 
                  for pharm_form in pharm_forms_to_add] 
    form_items = [Box([Label(value=pharm_form), pre_value[i_p]], layout=form_item_layout) for i_p, pharm_form in enumerate(pharm_forms_to_add)] 
    
    form_esklp_upd_unify_pharm_forms = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_esklp_upd_unify_pharm_forms, pre_value    

def form_param_esklp_exist_dicts(esklp_dates):
    esklp_dates_dropdown = widgets.Dropdown( options=esklp_dates) #, value=None)
    
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите дату сохраненного справочника ЕСКЛП:"), esklp_dates_dropdown], layout=form_item_layout) 
    form_items = [check_box]
    
    form_esklp_exist_dicts = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_esklp_exist_dicts, esklp_dates_dropdown 
