import json
from jsonpath import jsonpath
import os
import pandas as pd

def Refund(path):
    EventList = 'RefundEventList'
    df_RefundEventList = pd.DataFrame()

    for root, dirs, files in os.walk(path):
        for file in files:
            json_file = os.path.join(root, file)
            with open(json_file, 'r') as fp:
                data_raw = json.load(fp)
                # 从源数据data_raw中只选取EventList_5中的EventList --> data
                # 只讀ShipmentEvent --> data (type: list)
                data = data_raw.get(EventList)

                # 按照"PostedDate"和"AmazonOrderID"(如有)进行区分的数据 --> record (type: dict)
                for record in data:
                    # 根據record的内容塑造order_basic_dict, 與下一階段的record_name_amount_dicts鏈接
                    # 其中包含keys: OrderId, Date, Marketplace, Currency, Type, SKU, Quantity
                    order_basic_dict = {}
                    if record.get('OrderId') == None:
                        order_basic_dict['OrderId'] = record.get('AmazonOrderId')
                    else: order_basic_dict['OrderId'] = record.get('OrderId')

                    order_basic_dict['Date'] = record.get('PostedDate')
                    order_basic_dict['Marketplace'] = record.get('MarketplaceName')
                    order_basic_dict['Currency'] = jsonpath(record, '$..CurrencyCode')[0]
                    order_basic_dict['Type'] = EventList[:-9]

                    # 以上構造order_basic_dict結束，以下開始構造record_name_amount_dicts:
                    # ShipmentItemAdjustment為每條record之下的記錄，可能包含多條交易數據: len(ShipmentItemAdjustment)
                    # ShipmentItem --> type = list
                    ShipmentItemAdjustment = record['ShipmentItemAdjustmentList']

                    # 分解每一條交易數據，按照兩種情況：1) len(ShipmentItem) > 1 以及 2) len(ShipmentItem) = 1
                    for n in range(0, len(ShipmentItemAdjustment)):
                        item_rundown = ShipmentItemAdjustment[n]

                        name_amount_dicts = {}
                        name_amount_dicts['SKU'] = item_rundown['SellerSKU']
                        name_amount_dicts['Quantity'] = item_rundown['QuantityShipped'] * (-1)
                        # 新增Tax Collection Model
                        if jsonpath(item_rundown, '$..TaxCollectionModel') is not False:
                            name_amount_dicts['Tax Collection Model'] = jsonpath(item_rundown, '$..TaxCollectionModel')[0]
                        else: name_amount_dicts['Tax Collection Model'] = None

                        # 第n個交易中ItemChargeAdjustmentList中的Charge部分：
                        ItemChargeAdjustment = item_rundown.get('ItemChargeAdjustmentList')
                        if jsonpath(ItemChargeAdjustment, '$..ChargeType') is not False:
                            name1 = jsonpath(ItemChargeAdjustment, '$..ChargeType')
                            amount1 = jsonpath(ItemChargeAdjustment, '$..CurrencyAmount')
                            name_amount_dicts1 = dict(zip(name1, amount1))
                        else:
                            name_amount_dicts1 = {}
                        name_amount_dicts.update(name_amount_dicts1)

                        # 第n個交易中ItemFeeAdjustmentList的Fee部分
                        ItemFeeAdjustment = item_rundown.get('ItemFeeAdjustmentList')
                        if jsonpath(ItemFeeAdjustment, '$..FeeType') is not False:
                            name2 = jsonpath(ItemFeeAdjustment, '$..FeeType')
                            amount2 = jsonpath(ItemFeeAdjustment, '$..CurrencyAmount')
                            name_amount_dicts2 = dict(zip(name2, amount2))
                        else: name_amount_dicts2={}
                        name_amount_dicts.update(name_amount_dicts2)

                        # 第n個交易中PromotionAdjustmentList的Promotion部分
                        PromotionAdjustment = item_rundown.get('PromotionAdjustmentList')
                        name_amount_dicts3 = {}
                        if jsonpath(PromotionAdjustment, '$..PromotionType') is not False:
                            name3 = jsonpath(PromotionAdjustment, '$..PromotionType')
                            amount3 = jsonpath(PromotionAdjustment, '$..CurrencyAmount')
                            amount_sum = 0
                            for a in amount3:
                                amount_sum += a
                            name_amount_dicts3['PromotionMetaDataDefinitionValue'] = amount_sum
                        else: name_amount_dicts3 = {}
                        name_amount_dicts.update(name_amount_dicts3)

                        # 第n個交易中ItemTaxWithheldList中的Charge部分
                        ItemTaxWithheld = item_rundown.get('ItemTaxWithheldList')
                        if jsonpath(ItemTaxWithheld, '$..ChargeType') is not False:
                            name4 = jsonpath(ItemTaxWithheld, '$..ChargeType')
                            amount4 = jsonpath(ItemTaxWithheld, '$..CurrencyAmount')
                            name_amount_dicts4 = dict(zip(name4, amount4))
                        else: name_amount_dicts4 = {}
                        name_amount_dicts.update(name_amount_dicts4)

                        # JP專屬——CostOfPointsGranted
                        if jsonpath(item_rundown, '$..CostOfPointsReturned') is not False:
                            CostOfPointsGranted = item_rundown.get('CostOfPointsReturned')
                            name_amount_dicts['CostOfPointsReturned'] = jsonpath(CostOfPointsGranted, '$..CurrencyAmount')[0]
                        else: name_amount_dicts['CostOfPointsReturned'] = None

                        # 拼接一條交易的兩部分：order_dict = order_basic_dict 與 record_name_amount_dicts
                        order_dict = {}
                        order_dict.update(order_basic_dict)
                        order_dict.update(name_amount_dicts)

                        # 將order_dict輸出到csv
                        df_RefundEventList = df_RefundEventList.append(order_dict, ignore_index=True)
                        print('===========================一條交易輸出完畢=========================')
        return df_RefundEventList