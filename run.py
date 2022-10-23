from sqlalchemy import create_engine
import config, datetime, os, services, pandas as pd

basepath = os.getcwd()

class MainProcess:
    def __init__(self):
        self.citizenId = "3100501312521"
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
            citizenList = []
            nameList = []
            issueList = []
            # issueDeductList = []
            # issueTransactionList = []
            count = 0
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
                    # if not deduct:
                    #     issueDeductList.append(data['user_id'])
                    # if not accountTransaction:
                    #     issueTransactionList.append(data['user_id'])
                    if count + 1 == len(allList):
                        services.cooperativeService(
                            conn = self.conn,
                            dburl = config.Config.COOPERATIVE_DB_URL,
                            coopUserId = coopUserId
                        ).closeAccount()
                    self.trans.commit()
                    citizenList.append(user['personal_id'])
                    nameList.append(user['first_name'] + " " + user['last_name'])
                    count += 1
                    coopFund += self.fee
                    self.close()
                except Exception as error:
                    # print(error)
                    self.trans.rollback()
                    issueList.append(data['user_id'])
            # if len(issueList) == 0:
            #     services.cooperativeService(
            #         dburl = config.Config.COOPERATIVE_DB_URL,
            #         coopUserId = coopUserId
            #     ).closeAccount()
            infomation = [
                {
                    'รหัสบัตรประชาชนผูัเสียชีวืต' : self.citizenId,
                    'จำนวนกองทุนที่ได้รับ' : coopFund,
                    'จำนวนสมาชิกที่ถูกหักเงิน' : count
                }
            ]
            deductDetail = {
                'รหัสบัตรประชาชนสมาชิกที่ถูกหักเงิน' : citizenList,
                'ชื่อ-นามสกุล' : nameList
            }
            issueDetail = {
                'สมาชิกที่มีปัญหาในการหักเงิน' : issueList,
                # 'สมาชิกที่มีปัญหาเกี่ยวกับ Transaction' : issueTransactionList
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
