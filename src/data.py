from scipy.stats.stats import pearsonr
from typing import List

import json
import numpy as np
import os
import pydantic as pyd
import pandas as pd
import requests


## INPUT & OUTPUT PATHS
input_path_workorder = 'https://raw.githubusercontent.com/k-edge/data_engineering_challenge_fp/master/workorder.json'
output_path_workorder = './output_data/workorder.xlsx'

input_path_metrics = "https://raw.githubusercontent.com/k-edge/data_engineering_challenge_fp/master/metrics.json"
output_path_metrics = './output_data/metrics.xlsx'

## Static Report Path
Correlation_report = './output_data/Correlation_report.html'


## Create path for output files
if not os.path.exists('./output_data'):
    os.makedirs('./output_data')


## JSON Schema Validation
class workorder(pyd.BaseModel):
    time: int
    product: int
    production: float

    @pyd.validator('time')
    def time_must_be_positive(cls, v):
        if v < 0:
            raise ValueError('timestamp must be positive')
        return v


## JSON Schema Validation
class metrics(pyd.BaseModel):
    id: int
    val: float
    time: int

    @pyd.validator('time')
    def time_must_be_positive(cls, v):
        if v < 0:
            raise ValueError('timestamp must be positive')
        return v


## ETL Class interface
class _ingress_data_interface(): 
    def __init__(self, input, output):
        self._input_path = input
        self._output_path = output

    def read_data(self):
        raise NotImplementedError("read_data method is not implemented")

    def transform_data(self):
        raise NotImplementedError("transform_data method is not implemented")

    def write_data(self):
        raise NotImplementedError("write_data method is not implemented")


## JSON ETL Class
class json_ingress(_ingress_data_interface):
    def __init__(self, input, output, json_schema):
        self.json_schema = json_schema
        super().__init__(input, output)
    
    def get_data(self):
        r = requests.get(self._input_path)
        self._json_data = r.content
            
    
    def transform_data(self):
        try:
            json_item = json.loads(self._json_data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON: {exc.msg}, line {exc.lineno}, column {exc.colno}")

        try:
            self._pyd_output = pyd.parse_obj_as(List[self.json_schema], json_item)
        except pyd.ValidationError as exc:
            raise ValueError(f"ERROR: Invalid schema: {exc}")

    def write_data(self):
        pd.DataFrame([dict(s) for s in self._pyd_output]).to_excel(self._output_path, index=False)


def process_data(input_path: str, output_path: str, JSON_Schema):
    worker_etl = json_ingress(input_path, output_path, JSON_Schema)
    worker_etl.get_data()
    worker_etl.transform_data()
    worker_etl.write_data()


def get_data(path: str):
    data = pd.read_excel(path)
    return data

## Transforming the data for static html report
def transform_data_report(workorder_data: pd.DataFrame, metrics_data: pd.DataFrame) -> pd.DataFrame:
    product_list  = workorder_data['product'].unique().tolist()
    parameters_list = metrics_data['id'].unique().tolist()
    output_list = []

    for prod in product_list:
        list_correl = []
        for param in parameters_list:
            prod_data = workorder_data[workorder_data['product']==prod]
            param_data = metrics_data[metrics_data['id']==param]
            adjusted_param_data = np.interp(prod_data.time, param_data.time, param_data.val)
            cor, pval = pearsonr(prod_data.production, adjusted_param_data)
            list_correl.append((prod, param, cor, pval))
        
        list_correl.sort(key=lambda x: x[0],reverse=True)
        output_list.extend(list_correl[:3])

    output_results = pd.DataFrame(output_list, columns=['Product','Parameter','Correlation', 'P-Val'])
    output_results.sort_values(by=['Product','Correlation'], ascending=False, inplace=True)
    return output_results


def generate_html(data: pd.DataFrame):
    page_title_text='Correlation Analysis'
    table_text = 'Correlation of product with machine parameters'
    html = f'''
        <html>
            <head>
                <title>{page_title_text}</title>
            </head>
            <body>
                <h2>{table_text}</h2>
                {data.to_html(index=False)}
            </body>
        </html>
        '''
    # 3. Write the html string as an HTML file
    with open(Correlation_report, 'w') as f:
        f.write(html)


def generate_static_report():
    workorder_data = get_data(output_path_workorder)
    metrics_data = get_data(output_path_metrics)
    correlation_data = transform_data_report(workorder_data, metrics_data)
    generate_html(correlation_data)
    

def main():
    process_data(input_path_metrics, output_path_metrics, metrics)
    process_data(input_path_workorder, output_path_workorder, workorder)
    generate_static_report()

if __name__ == main():
    main()