from sqlalchemy import column
import config, datetime, json, os, services,xlsxwriter,pandas as pd 


class MainProcess:
    def __init__(self):
        self.citizenId = "3670200355171"
        self.fee = 100

    def process(self):
        coopFund = 0
        coopAccountId,coopUserId = services.cooperativeService(dburl = config.Config.COOPERATIVE_DB_URL,
                                           citizenId = self.citizenId).getIdAccountCoop()
        if coopAccountId == 'None' or coopUserId == 'None':
            print("This citizen id don't have info or sttus is not enable")
            exit()
        allList = services.cooperativeService(dburl = config.Config.COOPERATIVE_DB_URL,coopAccountId = coopAccountId,fee = self.fee).listCoop()
        idList = []
        count = 0
        for data in allList:
            count = count + 1
            coopFund = coopFund + self.fee
            idList.append(data['user_id'])
            services.cooperativeService(dburl = config.Config.COOPERATIVE_DB_URL,coopAccountId = data['id'],fee = self.fee).deduct()
            services.cooperativeService(dburl = config.Config.COOPERATIVE_DB_URL,coopAccountId = data['id'],fee = self.fee,dest_before_balance = data['balance'],citizenId = self.citizenId).accountTransaction()
            
        infomation =[{ 'รหัสบัตรประชาชนผูัเสียชีวืต': self.citizenId,'จำนวนกองทุนที่ได้รับ':coopFund,'จำนวนสมาชิกที่ถูกหักเงิน':count}]
        df1 = pd.DataFrame(infomation)
        df2 = pd.DataFrame(idList,columns=['ยูสเซอร์ไอดีสมาชิกที่ถูกหักเงิน'])
        with pd.ExcelWriter('output.xlsx') as writer:  
            df1.to_excel(writer, sheet_name='Sheet_name_1',index=False)
            df2.to_excel(writer, sheet_name='Sheet_name_2',index=False)
        services.cooperativeService(dburl = config.Config.COOPERATIVE_DB_URL,coopUserId = coopUserId).closeAccount()
        print("complete",datetime.datetime.now())
if __name__ == "__main__":
    MainProcess().process()
  