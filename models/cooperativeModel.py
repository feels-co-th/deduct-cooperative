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
        self.userId = kwargs.get("userId")
        self.autoCommit = False
        self.data = kwargs.get('data')
        self.conn = kwargs.get('conn')

    def connect(self):
        self.engine = create_engine(self.dburl)
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()
        self.isLocalConnect = True

    def close(self):
        self.conn.close()
        self.trans.close()
        self.engine.dispose()

    def getIdAccountCoop(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'personal_id': self.citizenId
        }
        sql = """
            SELECT * 
            FROM punsook_cooperative.account
            WHERE user_id =

            (
            SELECT id
            FROM punsook_cooperative.user
            WHERE personal_id = :personal_id 
            AND status = 'enable'
            )

            AND account_type = 2 
            AND status = 'enable'
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )
        records = result.fetchone()
        return records
    
    def getAllCoopAccount(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'id': self.coopAccountId,
            'fee':self.fee
        }
        sql = """
            SELECT * 
            FROM punsook_cooperative.account
            WHERE id != :id
            AND account_type = 2 
            AND status = 'enable' 
            AND balance > :fee
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )
        records = result.fetchall()
        return records
    
    def getCoopUser(self):
        engine = create_engine(self.dburl)
        conn = engine.connect()
        params = {
            'id': self.userId
        }
        sql = """
            SELECT * 
            FROM punsook_cooperative.user
            WHERE id = :id
        """
        result = conn.execute(
            text(sql).execution_options(autocommit=True),
            params
        )
        records = result.fetchone()
        return records

    def updateAccountCoop(self):
        if (self.conn is None):
            self.connect()
        try:
            params = {
                'id': self.coopAccountId,
                'fee': float(self.fee)
            }
            sql = """
                UPDATE punsook_cooperative.account
                SET balance = balance - :fee
                WHERE id = :id
                AND account_type = 2 
                AND status = 'enable' 
                AND balance > :fee

            """
            result = self.conn.execute(
                text(sql).execution_options(autocommit=True),
                params
            )
            result = result.rowcount
            if (self.isLocalConnect):
                self.trans.commit()
        except Exception as error:
            if (self.isLocalConnect):
                self.trans.rollback()
            raise str(error)
        finally:
            if (self.isLocalConnect):
                self.close()
        return result
        

    def insertAccountTransactionCoop(self):
        if (self.conn is None):
            self.connect()
        try:
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
            result = self.conn.execute(
                text(sql).execution_options(autocommit=True),
                params
            )
            result = result.lastrowid
            if (self.isLocalConnect):
                self.trans.commit()
        except Exception as error:
            if (self.isLocalConnect):
                self.trans.rollback()
            raise str(error)
        finally:
            if (self.isLocalConnect):
                self.close()
        return result

    def closeAccountCoop(self):
        if (self.conn is None):
            self.connect()
        try:
            params = {
                'id': self.coopUserId
            }
            sql = """
                UPDATE punsook_cooperative.account
                SET status = 'disable'
                WHERE user_id = :id
            """
            result = self.conn.execute(
                text(sql).execution_options(autocommit=True),
                params
            )
            result = result.rowcount
            if (self.isLocalConnect):
                self.trans.commit()
        except Exception as error:
            if (self.isLocalConnect):
                self.trans.rollback()
            raise str(error)
        finally:
            if (self.isLocalConnect):
                self.close()
        return result

    def closeUserCoop(self):
        if (self.conn is None):
            self.connect()
        try:
            params = {
                'id': self.coopUserId
            }
            sql = """
                UPDATE punsook_cooperative.user
                SET status = 'disable'
                WHERE id = :id
            """
            result = self.conn.execute(
                text(sql).execution_options(autocommit=True),
                params
            )
            result = result.rowcount
            if (self.isLocalConnect):
                self.trans.commit()
        except Exception as error:
            if (self.isLocalConnect):
                self.trans.rollback()
            raise str(error)
        finally:
            if (self.isLocalConnect):
                self.close()
        return result
