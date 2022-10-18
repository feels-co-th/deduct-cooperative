from sqlalchemy import create_engine, text
import datetime

class cooperativeModel:
    def __init__(self, **kwargs):
        self.dburl = kwargs.get("dburl")
        self.fee = kwargs.get("fee")
        self.citizenId = kwargs.get("citizenId")
        self.coopAccountId = kwargs.get("coopAccountId")
        self.dest_before_balance = kwargs.get("dest_before_balance")
        self.coopUserId = kwargs.get("coopUserId")
        self.now = datetime.datetime.now()

    def getIdAccountCoop(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'personal_id': self.citizenId
        }
        sql = """
            SELECT * FROM punsook_cooperative.account
            WHERE user_id =

            (SELECT id
            FROM punsook_cooperative.user
            WHERE personal_id = :personal_id AND status = 'enable')

            AND account_type = 2 AND status = 'enable'
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )
        records = result.fetchone()
          
        if not records:  
            records = {'id':'None',
                        'user_id':'None'}
        return records['id'],records['user_id']
    
    def getAllCoopAccount(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'id': self.coopAccountId,
            'fee':self.fee
        }
        sql = """
            SELECT * FROM punsook_cooperative.account
            WHERE id != :id
            AND account_type = 2 AND status = 'enable' AND balance > :fee
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )
        records = result.fetchall()
        return records

    def updateAccountCoop(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'id': self.coopAccountId,
            'fee': float(self.fee)
        }
        sql = """
            UPDATE punsook_cooperative.account
            SET balance = balance - :fee
            WHERE id = :id
            AND account_type = 2 AND status = 'enable' AND balance > :fee

        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )
        
        

    def updateAccountTransactionCoop(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'dest_id': self.coopAccountId,
            'amount': self.fee,
            'dest_before_balance': self.dest_before_balance,
            'citizenId' : self.citizenId
        }
        sql = """
            INSERT INTO punsook_cooperative.account_transaction (trans_type,dest_id,ratio,amount,dest_before_balance,created_date,remark)
            VALUES ('dec',:dest_id,'1',:amount,:dest_before_balance,NOW(),CONCAT('ถูกหักเนื่องจากสมาชิก',:citizenId,'เสียชีวิต'))
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )

    def closeAccountCoop(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'id': self.coopUserId
        }
        sql = """
            UPDATE punsook_cooperative.account
            SET status = 'disable'
            WHERE user_id = :id;
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        ) 
    def closeUserCoop(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'id': self.coopUserId
        }
        sql = """
            UPDATE punsook_cooperative.user
            SET status = 'disable'
            WHERE id = :id;
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        ) 
