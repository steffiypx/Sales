import json
from jsonpath import jsonpath
import os
import pandas as pd

def ProductAdsPayment(path):

    EventList = 'ProductAdsPaymentEventList'
    df_ProductAdsPaymentEventList = pd.DataFrame()

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
                    order_basic_dict['Date'] = record.get('postedDate')
                    order_basic_dict['Currency'] = jsonpath(record, '$..CurrencyCode')[0]
                    order_basic_dict['Type'] = EventList[:-9]
                    order_basic_dict['InvoiceId'] = record.get('invoiceId')

                    # 以上構造order_basic_dict結束，以下開始構造record_name_amount_dicts:
                    record_name_amount_dicts = {}
                    record_name_amount_dicts['BaseValue'] = record['baseValue']['CurrencyAmount']
                    record_name_amount_dicts['TaxValue'] = record['taxValue']['CurrencyAmount']

                    # 拼接一條record的兩部分：order_dict = order_basic_dict 與 record_name_amount_dicts
                    order_dict = {}
                    order_dict.update(order_basic_dict)
                    order_dict.update(record_name_amount_dicts)


                    # 將order_dict輸出到csv
                    df_ProductAdsPaymentEventList = df_ProductAdsPaymentEventList.append(order_dict, ignore_index=True)
                    print('===========================一條record輸出完畢=========================')
        return df_ProductAdsPaymentEventList