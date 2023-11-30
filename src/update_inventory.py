import os
import sys
import time
from dotenv import load_dotenv
from celery import Celery
from celery.result import AsyncResult
from sqlalchemy import insert, select
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from src.database import token_record

BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
celery_app = Celery('update_inventory', broker=BROKER_URL,
                    backend=RESULT_BACKEND)
DATABASE_URL_INVENTORY = os.getenv("DATABASE_URL_INVENTORY")
engine = create_engine(DATABASE_URL_INVENTORY)
Session = sessionmaker(bind=engine)

#Let assume that if there is no token_record in the database,
#initially, it will have 100 tokens available
@celery_app.task(name="update_inventory")
def update_inventory(payload: dict, fn: str):
    print("fn="+str(fn))
    print("payload="+str(payload))
    username: str = payload.get("username")
    quantity: int = payload.get("quantity")
    delivery: bool = payload.get("delivery")
    print("username="+str(username))
    print("quantity="+str(quantity))
    print("delivery="+str(delivery))
    print("db_url="+str(DATABASE_URL_INVENTORY))
    if(fn=="update_inventory"):
        print("checking available token in the inventory")
        session = Session()
        try:
            #query to check if there is any record in the token_record table
            query = select([token_record])
            result = session.execute(query)
            print("result="+str(result))
            print("result.rowcount="+str(result.rowcount))
            if result.rowcount == 0:
                print("no record in the token_record table")
                print("inserting record into the token_record table")
                initial_amt: int = 100
                if quantity > initial_amt:
                    print("quantity requested is more than the available token")
                    celery_app.send_task("create_payment", queue="q02", args=[payload, "rollback_payment"])
                    return "FAIL_INVENTORY (not enough token available)"
                else:
                    print("quantity requested is possible")
                    print("inserting record into the token_record table")
                    session.execute(
                        insert(token_record).values(
                            amount_available=initial_amt,
                            amount_taken=quantity,
                            amount_left=initial_amt-quantity,
                            username=username,
                        )
                    )
                    session.commit()
                    print("inserted first record into the token_record table successfully")
                   
                    #returning result to the order service
                    delivery_task = celery_app.send_task("make_delivery", queue="q04", args=[payload, "make_delivery"])
                    print("delivery_task.id="+str(delivery_task.id))
                    return waiting_delivery_result(delivery_task.id)
            else:
                print("there is record in the token_record table")
                #select the newest record
                query = select([token_record]).order_by(token_record.c.uuid.desc()).limit(1)
                result = session.execute(query).fetchone()  # Fetch the result row
                print("result="+str(result))
                if result.amount_left < quantity:
                    print("quantity requested is more than the available token")
                    print("inventory deduct fail .... rollbacking payment")
                    celery_app.send_task("create_payment", queue="q02", args=[payload, "rollback_payment"])
                    return "FAIL_INVENTORY (not enough token available)"
                else:
                    print("quantity requested is possible")
                    print("updating record in the token_record table")
                    deduct_token(username, quantity, delivery, result.amount_left)
                    
                    delivery_task = celery_app.send_task("make_delivery", queue="q04", args=[payload, "make_delivery"])
                    #returning result to the order service
                    print("delivery_task.id="+str(delivery_task.id))
                    return waiting_delivery_result(delivery_task.id)
                    
        except Exception as e:
            print(f"Error during database operation: {e}")
        finally:
            session.close()
    elif fn == "rollback_inventory":
        rollback_inventory(username, quantity, delivery)
        print("rollback_inventory successfully")
        print("inventory service sending task to rollback payment")
        celery_app.send_task("rollback_payment", queue="q02", args=[payload, "rollback_payment"])
    else:
        print("invalid function name in invnetory service kub")

@celery_app.task
def deduct_token(username: str, quantity: int, delivery: bool,amt_available: int):
    print("deducting token in inventory")
    print("amt_available="+str(amt_available)+" ,quantity="+str(quantity) + " ,amt_left="+str(int(amt_available-quantity)))
    session = Session()
    try:
        session.execute(
            insert(token_record).values(
                amount_available=amt_available,
                amount_taken=quantity,
                amount_left=amt_available-quantity,
                username=username,
            )
        )
        session.commit()
        print("successfully deducted token and updated record in the token_record table")
    except Exception as e:
        print(f"Error during database operation: {e}")
    finally:
        session.close()

@celery_app.task
def waiting_delivery_result(delivery_task_id):
    time.sleep(0.1)
    delivery_task_result = AsyncResult(delivery_task_id)
    if delivery_task_result.ready():
        result_value = delivery_task_result.result
        print(f"Task result: {result_value}")
        return result_value
    else:
        print("delivery task is still running...")
        return "delivery task is still running..."

@celery_app.task
def rollback_inventory(username: str, quantity: int, delivery: bool):
    print("rollback_inventory")
    session = Session()
    try:
        #select the newest record
        query = select([token_record]).order_by(token_record.c.uuid.desc()).limit(1)
        result = session.execute(query).fetchone()  # Fetch the result row
        current_amt_available: int = result.amount_left
        print("result_amt_left in rollback="+str(current_amt_available))
        new_amt_available: int = current_amt_available + quantity
        print("new_amt_available in rollback="+str(new_amt_available))
        session.execute(
            insert(token_record).values(
                amount_available=current_amt_available,
                amount_taken=quantity,
                amount_left=new_amt_available,
                username=username,
            )
        )
        session.commit()
    except Exception as e:
        print(f"Error during database operation: {e}")
    finally:
        session.close()

        
    
