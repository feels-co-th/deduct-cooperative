from sqlalchemy import create_engine
import config, datetime, os, services, pandas as pd

basepath = os.getcwd()

class MainProcess:
    def __init__(self):
        self.citizenId = "3341501538901"
        self.fee = 20
        self.List = []
        self.citizenList = []
        self.nameList = []
        self.issueList = []
        self.count = 0
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
            print("This citizen id don't have info or status is not enable")
        else:
            coopAccountId = recordAccountId['id']
            coopUserId = recordAccountId['user_id']
            balanceInAccount = recordAccountId['balance']
            allList = services.cooperativeService(
                dburl = config.Config.COOPERATIVE_DB_URL,
                coopAccountId = coopAccountId,
                fee = self.fee
            ).listCoop()
            coopFund = balanceInAccount
            for data in allList:
                try:
                    self.connect()
                    if data['user_id'] is None:
                        user = services.cooperativeService(
                            conn = self.conn,
                            dburl = config.Config.COOPERATIVE_DB_URL,
                            userId = data['account_term_id']
                        ).getUserCoop()
                    else:
                        user = services.cooperativeService(
                            conn = self.conn,
                            dburl = config.Config.COOPERATIVE_DB_URL,
                            userId = data['user_id']
                        ).getUserCoop()
                    services.cooperativeService(
                        conn = self.conn,
                        dburl = config.Config.COOPERATIVE_DB_URL,
                        coopAccountId = data['id'],
                        fee = self.fee
                    ).deduct()
                    services.cooperativeService(
                        conn = self.conn,
                        dburl = config.Config.COOPERATIVE_DB_URL,
                        coopAccountId = data['id'],
                        fee = self.fee,
                        dest_before_balance = data['balance'],
                        citizenId = self.citizenId
                    ).accountTransaction()
                    if self.count + 1 == len(allList):
                        services.cooperativeService(
                            conn = self.conn,
                            dburl = config.Config.COOPERATIVE_DB_URL,
                            coopUserId = coopUserId
                        ).closeAccount()
                    self.trans.commit()
                    self.citizenList.append(user['personal_id'])
                    self.nameList.append(user['first_name'] + " " + user['last_name'])
                    self.count += 1
                    coopFund += self.fee
                    self.close()
                except Exception as error:
                    # print(error)
                    self.trans.rollback()
                    self.issueList.append(data['user_id'])
            infomation = [
                {
                    'รหัสบัตรประชาชนผูัเสียชีวืต' : self.citizenId,
                    'จำนวนกองทุนที่ได้รับ' : coopFund,
                    'จำนวนสมาชิกที่ถูกหักเงิน' : self.count
                }
            ]
            deductDetail = {
                'รหัสบัตรประชาชนสมาชิกที่ถูกหักเงิน' : self.citizenList,
                'ชื่อ-นามสกุล' : self.nameList
            }
            issueDetail = {
                'สมาชิกที่มีปัญหาในการหักเงิน' : self.issueList
            }
            df1 = pd.DataFrame(infomation)
            df2 = pd.DataFrame(deductDetail)
            df3 = pd.DataFrame(issueDetail)
            with pd.ExcelWriter('{}/excels/{}.xlsx'.format(basepath, str(self.citizenId) + "-" + str(self.today))) as writer:
                df1.to_excel(writer, sheet_name='ข้อมูลผู้เสียชีวิต',index=False)
                df2.to_excel(writer, sheet_name='รายละเอียดผู้ร่วมทุน',index=False)
                df3.to_excel(writer, sheet_name='ผู้มีปัญหาระหว่างดำเนินการ',index=False)
            print("complete", datetime.datetime.now())
if __name__ == "__main__":
    MainProcess().process()
