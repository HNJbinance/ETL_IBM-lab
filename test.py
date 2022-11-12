import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import os

# CSV Extract Function
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)
    return dataframe
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name": name, "height": height, "weigth": weight}, ignore_index=True)
    return dataframe
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])
    # process all csv files
    for csvfile in glob.glob("*.csv"):

        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)

    # process all csv files
    for jsonfile in glob.glob("*.json"):

        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)

    # process all csv files
    for xmlfile in glob.glob("*.xml"):

        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)

    return extracted_data
def transform(data):
    # Convert height which is in inches to millimeter
    # Convert the datatype of the column into float
    # data.height = data.height.astype(float)
    # Convert inches to meters and round off to two decimals(one inch is 0.0254 meters)
    data['height'] = round(data.height * 0.0254, 2)

    # Convert weight which is in pounds to kilograms
    # Convert the datatype of the column into float
    # data.weight = data.weight.astype(float)
    # Convert pounds to kilograms and round off to two decimals(one pound is 0.45359237 kilograms)
    data['weight'] = round(data.weight * 0.45359237, 2)
    print(data)
    return data

def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


data = extract()
print(data)
#print(transform(data))

load("transformed.csv", data)

log("ETL Job Started")
log("Extract phase Started")
extracted_data = extract()

log("Extract phase Ended")
extracted_data

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")
transformed_data

log("Load phase Started")
load("transformed_data.csv", transformed_data)
log("Load phase Ended")
log("ETL Job Ended")
