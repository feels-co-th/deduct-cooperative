from pyexpat import model
from sqlalchemy import create_engine, text
import models


class cooperativeService:
    def __init__(self, **kwargs):
        self.dburl = kwargs.get("dburl")
        self.fee = kwargs.get("fee")
        self.citizenId = kwargs.get("citizenId")
        self.coopAccountId = kwargs.get("coopAccountId")
        self.dest_before_balance = kwargs.get("dest_before_balance")
        self.coopUserId = kwargs.get("coopUserId")
        self.userId = kwargs.get("userId")
        self.conn = kwargs.get('conn')

    def getIdAccountCoop(self):
        records = models.cooperativeModel(
            dburl=self.dburl,
            citizenId=self.citizenId
        ).getIdAccountCoop()
        return records

    def getUserCoop(self):
        records = models.cooperativeModel(
            conn=self.conn,
            dburl=self.dburl,
            userId=self.userId
        ).getCoopUser()
        return records

    def listCoop(self):
        allList = models.cooperativeModel(
            dburl=self.dburl,
            coopAccountId = self.coopAccountId,
            fee = self.fee
        ).getAllCoopAccount()
        return allList

    def deduct(self):
        result = models.cooperativeModel(
            conn=self.conn,
            dburl=self.dburl,
            coopAccountId = self.coopAccountId,
            fee = self.fee  
        ).updateAccountCoop()
        return result

    def accountTransaction(self):
        result = models.cooperativeModel(
            conn=self.conn,
            dburl=self.dburl,
            coopAccountId = self.coopAccountId,
            fee = self.fee,
            dest_before_balance = self.dest_before_balance,
            citizenId = self.citizenId
        ).insertAccountTransactionCoop()
        return result

    def closeAccount(self):
        models.cooperativeModel(
            dburl=self.dburl,
            coopUserId = self.coopUserId).closeAccountCoop()
        models.cooperativeModel(
            dburl=self.dburl,
            coopUserId = self.coopUserId).closeUserCoop()