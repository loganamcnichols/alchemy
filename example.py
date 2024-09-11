from dotenv import load_dotenv
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re

import os

from datetime import datetime
import time



# Substitute these with your API details
load_dotenv()
api_key = os.environ.get('API_KEY')
api_secret = os.environ.get('API_SECRET')

survey_id = '7982666'
#file_loc = "~/downloads/20240605103104-SurveyExport.csv"
file_loc='test.csv'

kill_partials = np.nan#0.8 # proportion of survey

weighting_targets = np.nan

custom_code_file =np.nan #"/Users/michaelsadowsky/documents/omnibus_nov_9.py"


def convert_to_seconds(time_str):
    dt = datetime.strptime(time_str, '%b %d, %Y %I:%M:%S %p')
    seconds = int(time.mktime(dt.timetuple()))
    return seconds

def calc_weighted_val(group):
    return (group['val'] * group['weight']).sum() / group['weight'].sum()


test = pd.read_csv(file_loc)




# Endpoint to get survey details (you might need to update this depending on the specific API endpoint you need)
url = f'https://api.alchemer.com/v5/survey/{survey_id}?api_token={api_key}&api_token_secret={api_secret}'
response = requests.get(url)

data = response.json()

d=data['data']

qdict = {'variable_name':[],'text':[],'type':[],'master':[],'qid':[],'grid_name':[]}

def replace_non_alphanumeric(input_string):
    ret = re.sub(r'[^a-zA-Z0-9]', '_', input_string)
    if ret.startswith('X'):
        ret = ret[1:]
    return ret


option_dict = {}

for pg in d['pages']:
    for quest in pg['questions']:
        if 'English' in str(quest):
            if 'options' in quest and quest['type'] in ['RADIO','MENU']:
                qdict['variable_name'].append(quest['shortname'])
                qdict['text'].append(quest['title']['English'])
                qdict['grid_name'].append(np.nan)
                qdict['type'].append(quest['type'])
                qdict['master'].append(np.nan)
                qdict['qid'].append(quest['id'])
                option_dict[quest['shortname']]=[]
                options = quest['options']
                for opt in options:
                    option_dict[quest['shortname']].append(opt['value'])
            if 'options' in quest and quest['type'] in ['CHECKBOX']:
                orig_options = quest['options']
                for oopt in orig_options:
                    ovar_name =  replace_non_alphanumeric(oopt['value']) + '_' + quest['shortname']
                    qdict['variable_name'].append(ovar_name)
                    qdict['text'].append(quest['title']['English']+' {0}'.format(oopt['title']['English']))
                    qdict['grid_name'].append(np.nan)
                    qdict['type'].append(quest['type'])
                    qdict['master'].append(quest['shortname'])
                    qdict['qid'].append(quest['id'])
            if 'options' in quest and quest['type'] in ['TABLE'] and pd.notnull(quest['shortname']):
                subquests = quest['sub_questions']
                options = quest['options']
                for subq in subquests:
                    ovar_name =  replace_non_alphanumeric(subq['title']['English']) + '_' + quest['shortname']
                    qdict['variable_name'].append(ovar_name)
                    qdict['text'].append(quest['title']['English']+' {0}'.format(subq['title']['English']))
                    qdict['grid_name'].append(replace_non_alphanumeric(subq['title']['English']+' '+quest['title']['English']))
                    qdict['type'].append(quest['type'])
                    qdict['master'].append(np.nan)
                    qdict['qid'].append(quest['id'])
                    option_dict[ovar_name]=[]
                    for opt in options:
                        option_dict[ovar_name].append(opt['value'])
            if quest['type']=='HIDDEN':
                qdict['variable_name'].append(quest['title']['English'])
                qdict['text'].append(np.nan)
                qdict['grid_name'].append(np.nan)
                qdict['type'].append('HIDDEN')
                qdict['master'].append(np.nan)
                qdict['qid'].append(quest['id'])
            if quest['type']=='TEXTBOX':
                qdict['variable_name'].append(quest['shortname'])
                qdict['text'].append(quest['title']['English'])
                qdict['grid_name'].append(np.nan)
                qdict['type'].append('open_end')
                qdict['master'].append(np.nan)
                qdict['qid'].append(quest['id'])
                    
                    
                    
qdf = pd.DataFrame(qdict)


def strip_text(x):
    try:
        x = BeautifulSoup(x, "html.parser").get_text().replace('\n', '<br/><br/>')
    except:
        pass
    return x

qdf['text']=qdf['text'].apply(lambda x: strip_text(x))



qdf=qdf[qdf['variable_name'].apply(lambda x: len(str(x)))>0]
qdf['variable_name'] = qdf['variable_name'].apply(lambda x: 'X' + x if str(x)[0].isdigit() else x)
qdf = qdf[qdf.variable_name.notnull()]
qdf = qdf.drop_duplicates()
qvars = list(qdf['variable_name'])

    

    


bad = [c for c in list(qdf.variable_name) if c not in test]
good = [c for c in list(qdf.variable_name) if c in test]


def replace_non_alphanumeric(input_string):
    ret = re.sub(r'[^a-zA-Z0-9]', '_', input_string)
    if ret.startswith('X'):
        ret = ret[1:]
    return ret

gcols = list(test.columns)
gcols = [replace_non_alphanumeric(g) for g in gcols]
test.columns = gcols

bad_new = [c for c in list(qdf.variable_name) if c not in test]
good_new = [c for c in list(qdf.variable_name) if c in test]

test.to_csv("~/documents/survey_output.csv",index=False)