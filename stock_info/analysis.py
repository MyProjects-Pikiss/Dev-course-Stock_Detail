import os, csv
import pandas as pd

def KoreaExchange(price):
    return round(price * 0.00073438, 2)
    
def JapanExchange(price):
    return round(price * 0.00640233, 2)

def ChinaExchange(price):
    return round(price * 0.13845997, 2)

def AnanlyzeData():
    data_folder = './data'
    data = os.listdir(data_folder)
    if '.DS_Store' in data:
        data.remove('.DS_Store')
        
    company_name_list = []
    data_path_list = []
    for i in data:
        company_name_list.append(i.replace('.csv', ''))
    for i in data:
        data_path_list.append(data_folder + '/' + i)
    for i in range(len(data_path_list)):
        f = open(data_path_list[i], 'r')
        reader = csv.reader(f)
        name = company_name_list[i]
        stock_list = {
            'COMPANY_NAME': [],
            'BASE_DATE': [],
            'STOCK_PRICE': [],
            'FLUCTUATION_VALUE': [],
            'FLUCTUATION_CHECK': [],
        }
        count = -1
        for line in reader:
            count += 1
            if count == 0:
                continue
            stock_list['COMPANY_NAME'].append(name)
            stock_list['BASE_DATE'].append(line[0].replace(' ',''))
            if name == 'nikkei':
                stock_list['STOCK_PRICE'].append(JapanExchange(float(line[1].replace(',', ''))))
            elif name == 'kosdaq' or name == 'kospi':
                stock_list['STOCK_PRICE'].append(KoreaExchange(float(line[1].replace(',', ''))))
            elif name == 'szi':
                stock_list['STOCK_PRICE'].append(ChinaExchange(float(line[1].replace(',', ''))))
            else:
                stock_list['STOCK_PRICE'].append(float(line[1].replace(',', '')))
            stock_list['FLUCTUATION_VALUE'].append(float(line[6].replace('%', '')))
            if float(line[6].replace('%', '')) < 0:
                stock_list['FLUCTUATION_CHECK'].append(-1)
            elif float(line[6].replace('%', '')) == 0:
                stock_list['FLUCTUATION_CHECK'].append(0)
            else:
                stock_list['FLUCTUATION_CHECK'].append(1)
        stock_data = pd.DataFrame(stock_list)
        stock_data.to_csv("./analysis_data/" + company_name_list[i] + "_analaysis.csv", index=False)

if __name__=='__main__':
    AnanlyzeData()
    