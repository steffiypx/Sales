from FinancialEvents2 import *
import pandas as pd

# 注意調整輸入的path和輸出的file
path = r'C:\Users\Steffi\Desktop\HH Power BI_Data\HH后台原始数据\0_API\financial_events\2022-08-01'
file = r'C:\Users\Steffi\Desktop\HH Power BI_Data\HH后台原始数据\0_API\financial_events\HH_Sales_2022-08-01.csv'

df_allEventList = pd.DataFrame()

df_Shipment = ShipmentEventList.Shipment(path)
df_allEventList = pd.concat([df_allEventList, df_Shipment])

df_Refund = RefundEventList.Refund(path)
df_allEventList = pd.concat([df_allEventList, df_Refund])

df_RemovalShipment = RemovalShipmentEventList.Removal(path)
df_allEventList = pd.concat([df_allEventList, df_RemovalShipment])

df_ServiceFee = ServiceFeeEventList.ServiceFee(path)
df_allEventList = pd.concat([df_allEventList, df_ServiceFee])

df_CouponPayment = CouponPaymentEventList.CouponPayment(path)
df_allEventList = pd.concat([df_allEventList, df_CouponPayment])

df_Retrocharge = RetrochargeEventList.Retrocharge(path)
df_allEventList = pd.concat([df_allEventList, df_Retrocharge])

df_ProductAdsPayment = ProductAdsPaymentEventList.ProductAdsPayment(path)
df_allEventList = pd.concat([df_allEventList, df_ProductAdsPayment])

df_Adjustment = AdjustmentEventList.Adjustment(path)
df_allEventList = pd.concat([df_allEventList, df_Adjustment])

df_RemovalShipmentAdjustment = RemovalShipmentAdjustmentEventList.RemovalShipmentAdjustment(path)
df_allEventList = pd.concat([df_allEventList, df_RemovalShipmentAdjustment])

df_NetworkComminglingTransaction = NetworkComminglingTransactionEventList.NetworkComminglingTransaction(path)
df_allEventList = pd.concat([df_allEventList, df_NetworkComminglingTransaction])

## 按照文件夾的名字（取數時間）為DataFrame增加一列Month
df_allEventList['Month'] = path[-10:]

## 區分df_allEventList中的自營與代運營信息
代运营sku信息 = pd.read_excel(r'C:\Users\Steffi\Desktop\客户流水读取\代运营信息.xlsx', sheet_name= "代运营SKU信息")
# 在df_allEventList中新建列Client
df_allEventList['Client'] =''

# Step1: 找到Client的CouponPayment——如果description列包含代运营ASIN，在已有['ASIN']列添加该ASIN，其他为空值
ASIN = list(代运营sku信息.loc[:, 'ASIN'].dropna())
MSKU = list(代运营sku信息.loc[:, 'MSKU'].dropna())
FNSKU = list(代运营sku信息.loc[:, 'FNSKU'].dropna())
des = df_allEventList['Description'].str.rsplit(";").str.get(-1)
df_allEventList['ASIN'] = des.apply(lambda x: x if x in ASIN else None)

# Step2.1：將OrderId與SKU對應——創建鍵值對，OrderId為Key，SKU為Value，【去掉Value爲空的部分】
OrderId_SKU = df_allEventList.set_index(['OrderId'])['SKU'].to_dict()
for key, value in dict(OrderId_SKU).items():
    if value is None:
        del OrderId_SKU
# Step2.2：儅鍵值對的Value在MSKU或者FNSKU時，【在對應列填補Value】
for o in OrderId_SKU.keys():
    if OrderId_SKU[o] in MSKU:
        df_allEventList.loc[df_allEventList['OrderId'] == o, 'SKU'] = OrderId_SKU[o]
for o in OrderId_SKU.keys():
    if OrderId_SKU[o] in FNSKU:
        df_allEventList.loc[df_allEventList['OrderId'] == o, 'SKU'] = OrderId_SKU[o]

# Step3：用dict創建"代運營sku信息"對應的鍵值對，通過df_allEventList中的【SKU】列與【ASIN】列對應【Client】
ASIN_Client = 代运营sku信息.set_index(['ASIN'])['客户简称'].to_dict()
MSKU_Client = 代运营sku信息.set_index(['MSKU'])['客户简称'].to_dict()
FNSKU_Client = 代运营sku信息.set_index(['FNSKU'])['客户简称'].to_dict()

for a in df_allEventList['ASIN']:
    if a in ASIN:
        df_allEventList.loc[df_allEventList['ASIN'] ==a, 'Client'] = ASIN_Client[a]
for s in df_allEventList['SKU']:
    if s in MSKU:
        df_allEventList.loc[df_allEventList['SKU'] ==s, 'Client'] = MSKU_Client[s]
for s in df_allEventList['SKU']:
    if s in FNSKU:
        df_allEventList.loc[df_allEventList['SKU'] ==s, 'Client'] = FNSKU_Client[s]


# 當csv文件中【沒有内容】時：
df_allEventList.to_csv(file, index = False)
# # # 當csv文件中【有内容】時：
# # df_allEventList.to_csv(file, mode = 'a', index = False)

print('對[Client]列空值手動填充為【自营】')
print('**********************************************************')
print('篩選[Month]列，查看【Adjustment】（特別是JP的）是否有數據移位並調整')