import camelot
import pandas as pd 

import glob
from pathlib import Path
import re


project_dir = Path(__file__).resolve().parents[2]
raw_data_folder = project_dir.joinpath(
    "data",
    "raw",
    "yearly",
    "2006-2007"
)
title_pattern = re.compile(r'.*\d\.\d.*')

airline_name_dict = {
    "ic": "INDIAN AIRLINES",
    "cd": "ALLIANCE AIR",
    "ic+cd": "INDIAN AIRLINES ALLIANCE AIR",
    "AI": "AIR INDIA",
    "AI+AIEXPRESS": "AIR INDIA - AIR INDIA EXPRESS",
    "AI EXPRESS": "AIR INDIA EXPRESS",
    "chap-3": "SCHEDULED DOMESTIC AIRLINES OTHER THAN AIR INDIA AND INDIAN AIRLINES",
    "chap-4": "TOTAL TRAFFIC STATISTICS OF SCHEDULED INDIAN CARRIERS",
    "CHAP-5": "AIRPORT STATISTICS",
    "CHAP-6": "NON-SCHEDULED TRAFFIC CARRIED BY PRIVATE OPERATORS AND SCHEDULED OPERATORS",
    "CHAP-7": "SCHEDULED INTERNATIONAL OPERATIONS",
    "CHAP-8": "TOURIST CHARTERS OPERATIONS AND CARGO STATISTICS UNDER OPEN SKY POLICY",
}


def get_files():
    tables =  glob.glob(str(raw_data_folder) + "/complete-*/*/*/[!Title]*.pdf") + glob.glob(str(raw_data_folder) + "/complete-*/*/*/*/[!Title]*.pdf")
    for each_path in tables :
        yield each_path


def read_pdf(file_path):
    tables = camelot.read_pdf(file_path, pages = "1-end")
    for table in tables :
        yield table


def get_table_name(df):
    """
    Read table heading from dataframe obtained via tabula
    """
    trial_val = [val for val in df.iloc[:5].values.flatten() if isinstance(val,str)] 
    title_val = [val for val in trial_val if not re.search(title_pattern,val) and len(val) > 30 and 'cid' not in val] 
    if len(title_val) > 0:
        return title_val[0]
    else :
        return trial_val[2]


def text_modification(name):
    """
    Make proper folder name and file names
    1) Use ReGex to remove Special Characters
    2) LowerCase
    3) Substitute Spaces " " -> Hyphen "-"
    """

    # remove whitespace and provide casing
    temp_name = (
        name.strip()
        .lower()
        .replace(".", "")
        .replace("\r", " ")
        .replace("   ", " ")
        .replace("  ", " ")
    )

    # use regex to substitute
    sub_name = re.sub(r"[^0-9a-z-]", "-", temp_name.lower())
    sub_name = (
        sub_name.replace("---","-")
        .replace("--","-")
    )
    return sub_name
    

def save_csv(df, file_path, table_name):
    # make proper output path
    airline_name = file_path.split('pub06-07/')[-1].split('/')[0]
    mod_airline_name = text_modification(
        airline_name_dict[airline_name]
    )

    mod_table_name = text_modification(table_name)

    split_path = file_path.split('/2006-2007/')
    output_path = Path(split_path[0].replace('/raw/','/interim/')+'/'+"2006-2007"+'/'+ mod_airline_name + '/' + mod_table_name)
    output_file_path = output_path.joinpath('output.csv')
    
    # consition on basis of output path 
    if output_file_path.is_file():
        print("File Already exist : ", mod_airline_name + ' --> ' + mod_table_name)
        mode = 'a'
    else :
        output_path.mkdir(parents=True, exist_ok=True)
        mode = 'w'
    
    # convert dataframe to CSV
    df.to_csv(str(output_path) + "/output.csv", index=False, mode=mode)

if __name__ == "__main__": 

    for each_path in get_files():
        for dataset in read_pdf(each_path):
            
            try :
                frame = dataset.df
                title = get_table_name(frame)
                save_csv(frame, each_path, title)
            
            except Exception as e:
                print("== == == == ==", each_path.split('2007/')[-1], ": ", e)
                with open("./err.txt", "a") as error:
                    error.write(str(each_path.split('2007/')[-1]) + "\n")
