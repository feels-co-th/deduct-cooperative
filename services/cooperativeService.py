from pyexpat import model
import models


class cooperativeService:
    def __init__(self, **kwargs):
        self.dburl = kwargs.get("dburl")
        self.fee = kwargs.get("fee")
        self.citizenId = kwargs.get("citizenId")
        self.coopAccountId = kwargs.get("coopAccountId")
        self.dest_before_balance = kwargs.get("dest_before_balance")
        self.coopUserId = kwargs.get("coopUserId")

    def getIdAccountCoop(self):
        id,user_id = models.cooperativeModel(
            dburl=self.dburl,
            citizenId=self.citizenId
        ).getIdAccountCoop()
        return id,user_id

    def listCoop(self):
        allList = models.cooperativeModel(
            dburl=self.dburl,
            coopAccountId = self.coopAccountId,
            fee = self.fee
        ).getAllCoopAccount()
        return allList

    def deduct(self):
        models.cooperativeModel(
            dburl=self.dburl,
            coopAccountId = self.coopAccountId,
            fee = self.fee  
        ).updateAccountCoop()
        
    
    def accountTransaction(self):
        models.cooperativeModel(
            dburl=self.dburl,
            coopAccountId = self.coopAccountId,
            fee = self.fee,
            dest_before_balance = self.dest_before_balance,
            citizenId = self.citizenId
        ).updateAccountTransactionCoop()
    
    def closeAccount(self):
        models.cooperativeModel(dburl=self.dburl,
                                coopUserId = self.coopUserId).closeAccountCoop()
        models.cooperativeModel(dburl=self.dburl,
                                coopUserId = self.coopUserId).closeUserCoop()
        
        