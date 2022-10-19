from sqlalchemy import create_engine,column
import config, datetime, os, services, pandas as pd 
basepath = os.getcwd()

class MainProcess:
    def __init__(self):
        self.citizenId = "3341501538901"
        self.fee = 20
        self.List = []
        self.today = datetime.date.today()
    def process(self):
        recordAccountId = services.cooperativeService(
            dburl = config.Config.COOPERATIVE_DB_URL,
            citizenId = self.citizenId
            ).getIdAccountCoop()
        if recordAccountId is None:
            print("This citizen id don't have info or sttus is not enable")
        else:
            coopAccountId = recordAccountId['id']
            coopUserId = recordAccountId['user_id']
            balanceInAccount = recordAccountId['balance']
            allList = services.cooperativeService(
                dburl = config.Config.COOPERATIVE_DB_URL, 
                coopAccountId = coopAccountId,
                fee = self.fee
                ).listCoop()
            citizenList = []
            nameList = []
            for data in allList:
                if data['user_id'] is None:
                    user = services.cooperativeService(
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    userId = data['account_term_id']
                    ).getUserCoop()
                else:
                    user = services.cooperativeService(
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    userId = data['user_id']
                    ).getUserCoop()
                citizenList.append(user['personal_id'])
                nameList.append(user['first_name'] + " " + user['last_name'])
                services.cooperativeService(
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    coopAccountId = data['id'],
                    fee = self.fee
                    ).deduct()
                services.cooperativeService(
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    coopAccountId = data['id'],
                    fee = self.fee,
                    dest_before_balance = data['balance'],
                    citizenId = self.citizenId
                    ).accountTransaction()
            count = len(allList)
            coopFund = balanceInAccount + (count * self.fee)
            infomation =[{ 
                'รหัสบัตรประชาชนผูัเสียชีวืต': self.citizenId,
                'จำนวนกองทุนที่ได้รับ':coopFund,
                'จำนวนสมาชิกที่ถูกหักเงิน':count
                }]
            df1 = pd.DataFrame(infomation)
            df2 = pd.DataFrame({'รหัสบัตรประชาชนสมาชิกที่ถูกหักเงิน':citizenList,'ชื่อ-นามสกุล':nameList})
            with pd.ExcelWriter('{}/excels/{}.xlsx'.format(basepath,str(self.citizenId) + "-" + str(self.today))) as writer:  
                df1.to_excel(writer, sheet_name='Sheet_name_1',index=False)
                df2.to_excel(writer, sheet_name='Sheet_name_2',index=False)
            services.cooperativeService(
                dburl = config.Config.COOPERATIVE_DB_URL,
                coopUserId = coopUserId
                ).closeAccount()
            print("complete",datetime.datetime.now())
if __name__ == "__main__":
    MainProcess().process()
  