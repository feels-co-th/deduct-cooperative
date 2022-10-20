from sqlalchemy import create_engine,column
import config, datetime, os, services, pandas as pd 
basepath = os.getcwd()

class MainProcess:
    def __init__(self):
        self.citizenId = "3341501538901"
        self.fee = 20
        self.List = []
        self.today = datetime.date.today()
        self.dburl = config.Config.COOPERATIVE_DB_URL

    def connect(self):
        self.engine = create_engine(self.dburl)
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()
        self.isLocalConnect = True

    def close(self):
        self.conn.close()
        self.trans.close()
        self.engine.dispose()

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
            issueList = []
            for data in allList:
                self.connect()
                if data['user_id'] is None:
                    user = services.cooperativeService(
                    conn=self.conn,
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    userId = data['account_term_id']
                    ).getUserCoop()
                else:
                    user = services.cooperativeService(
                    conn=self.conn,
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    userId = data['user_id']
                    ).getUserCoop()
                citizenList.append(user['personal_id'])
                nameList.append(user['first_name'] + " " + user['last_name'])
                deduct = services.cooperativeService(
                    conn=self.conn,
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    coopAccountId = data['id'],
                    fee = self.fee
                    ).deduct()
                accountTransaction =services.cooperativeService(
                    conn=self.conn,
                    dburl = config.Config.COOPERATIVE_DB_URL,
                    coopAccountId = data['id'],
                    fee = self.fee,
                    dest_before_balance = data['balance'],
                    citizenId = self.citizenId
                    ).accountTransaction()
                if not deduct:
                    issueList.append(data['user_id'])
                self.trans.commit()
                self.close()
            count = len(allList)
            coopFund = balanceInAccount + (count * self.fee)
            infomation =[{ 
                'รหัสบัตรประชาชนผูัเสียชีวืต': self.citizenId,
                'จำนวนกองทุนที่ได้รับ':coopFund,
                'จำนวนสมาชิกที่ถูกหักเงิน':count
                }]
            df1 = pd.DataFrame(infomation)
            df2 = pd.DataFrame({'รหัสบัตรประชาชนสมาชิกที่ถูกหักเงิน':citizenList,'ชื่อ-นามสกุล':nameList})
            df3 = pd.DataFrame({'สมาชิกที่มีปัญหาในการหักเงิน':issueList})
            with pd.ExcelWriter('{}/excels/{}.xlsx'.format(basepath,str(self.citizenId) + "-" + str(self.today))) as writer:  
                df1.to_excel(writer, sheet_name='Sheet_name_1',index=False)
                df2.to_excel(writer, sheet_name='Sheet_name_2',index=False)
                df3.to_excel(writer, sheet_name='Sheet_name_3',index=False)
            services.cooperativeService(
                dburl = config.Config.COOPERATIVE_DB_URL,
                coopUserId = coopUserId
                ).closeAccount()
            print("complete",datetime.datetime.now())
if __name__ == "__main__":
    MainProcess().process()
