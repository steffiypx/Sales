import json
from jsonpath import jsonpath
import os
import pandas as pd


def Retrocharge(path):
    EventList = 'RetrochargeEventList'
    df_RetrochargeEventList = pd.DataFrame()

    for root, dirs, files in os.walk(path):
        for file in files:
            json_file = os.path.join(root, file)
            with open(json_file, 'r') as fp:
                data_raw = json.load(fp)
                # 从源数据data_raw中只选取EventList_5中的EventList --> data
                # 只讀ShipmentEvent --> data (type: list)
                data = data_raw.get(EventList)
                for record in data:
                    # 根據record的内容塑造order_basic_dict, 與下一階段的record_name_amount_dicts鏈接
                    # 其中包含keys: OrderId, Date, Marketplace, Currency, Type, SKU, Quantity
                    order_basic_dict = {}
                    order_basic_dict['Date'] = record.get('PostedDate')
                    order_basic_dict['Currency'] = jsonpath(record, '$..CurrencyCode')[0]
                    order_basic_dict['Type'] = EventList[:-9]
                    order_basic_dict['Marketplace'] = record.get('MarketplaceName')
                    if jsonpath(record, '$..TaxCollectionModel') is not False:
                        order_basic_dict['Tax Collection Model'] = jsonpath(record, '$..TaxCollectionModel')[0]
                    else: order_basic_dict['Tax Collection Model'] = None

                    # 以上構造order_basic_dict結束，以下開始構造record_name_amount_dicts:
                    record_name_amount_dicts = {}
                    # csv中只有的marketplace withheld tax數據，刪除BaseTax與ShippingTax部分，只保留
                    # record_name_amount_dicts['BaseTax'] = record['BaseTax']['CurrencyAmount']
                    # record_name_amount_dicts['ShippingTax'] = record['ShippingTax']['CurrencyAmount']
                    name = jsonpath(record, '$..ChargeType')[0]
                    record_name_amount_dicts[name] = record['RetrochargeTaxWithheldList'][0]['TaxesWithheld'][0]['ChargeAmount']['CurrencyAmount']

                    # 拼接一條record的兩部分：order_dict = order_basic_dict 與 record_name_amount_dicts
                    order_dict = {}
                    order_dict.update(order_basic_dict)
                    order_dict.update(record_name_amount_dicts)


                    # 將order_dict輸出到csv
                    df_RetrochargeEventList = df_RetrochargeEventList.append(order_dict, ignore_index=True)
                    print('===========================一條record輸出完畢=========================')
        return df_RetrochargeEventList