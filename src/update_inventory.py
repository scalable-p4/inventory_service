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

# OpenTelemetry imports for tracing and metrics
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.celery import CeleryInstrumentor

# Logging imports
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
import logging
service_name = "inventory_worker"

# Initialize TracerProvider for OTLP
resource = Resource(attributes={SERVICE_NAME: service_name})
trace_provider = TracerProvider(resource=resource)
otlp_trace_exporter = OTLPSpanExporter(endpoint="otel-collector:4317", insecure=True)
trace_provider.add_span_processor(BatchSpanProcessor(otlp_trace_exporter))
trace.set_tracer_provider(trace_provider)

# Initialize MeterProvider for OTLP
metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint="otel-collector:4317", insecure=True))
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

# Initialize LoggerProvider for OTLP
logger_provider = LoggerProvider(resource=resource)
set_logger_provider(logger_provider)
otlp_log_exporter = OTLPLogExporter(endpoint="otel-collector:4317", insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
handler = LoggingHandler(level=logging.DEBUG, logger_provider=logger_provider)

# Attach OTLP handler to root logger
logging.getLogger().addHandler(handler)

tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
celery_app = Celery('update_inventory', broker=BROKER_URL,
                    backend=RESULT_BACKEND)
DATABASE_URL_INVENTORY = os.getenv("DATABASE_URL_INVENTORY")
engine = create_engine(DATABASE_URL_INVENTORY)
Session = sessionmaker(bind=engine)

CeleryInstrumentor().instrument()

inventory_counter = meter.create_counter(
    "inventory_updated",
    description="Total number of inventory updated",
    unit="1",
)
inventory_commit_counter = meter.create_counter(
    "inventory_committed",
    description="Total number of committed inventory",
    unit="1",
)
inventory_rollback_counter = meter.create_counter(
    "inventory_rollback",
    description="Total number of rollback inventory",
    unit="1",
)
token_check_counter = meter.create_counter(
    "token_checks",
    description="Total number of token checks",
    unit="1",
)
delivery_results_counter = meter.create_counter(
    "delivery_results_waited",
    description="Counts how many times delivery results were waited for",
    unit="1",
)

#Let assume that if there is no token_record in the database,
#initially, it will have 100 tokens available
@celery_app.task(name="update_inventory")
def update_inventory(payload: dict, fn: str):
    with tracer.start_as_current_span("update_inventory_task"):
        logger.info("Updating inventory", extra={"payload": payload, "function": fn})
        inventory_counter.add(1)

        print("fn="+str(fn))
        print("payload="+str(payload))
        username: str = payload.get("username")
        quantity: int = payload.get("quantity")
        delivery: bool = payload.get("delivery")
        print("username="+str(username))
        print("quantity="+str(quantity))
        print("delivery="+str(delivery))
        print("db_url="+str(DATABASE_URL_INVENTORY))
        logger.info(f"Inventory details: username={username}, quantity={quantity}, delivery={delivery}")
        if(fn=="update_inventory"):
            print("checking available token in the inventory")
            token_check_counter.add(1)
            session = Session()
            try:
                #query to check if there is any record in the token_record table
                logger.info("Checking record in the token_record table")
                query = select([token_record])
                result = session.execute(query)
                print("result="+str(result))
                print("result.rowcount="+str(result.rowcount))
                if result.rowcount == 0:
                    logger.info("No record in the token_record table")
                    logger.info("Inserting record into the token_record table")
                    initial_amt: int = 100
                    if quantity > initial_amt:
                        logger.info("Quantity requested is more than the available token")
                        celery_app.send_task("create_payment", queue="q02", args=[payload, "rollback_payment"])
                        return "FAIL_INVENTORY (not enough token available)"
                    else:
                        logger.info("Quantity requested is possible")
                        logger.info("Inserting record into the token_record table")
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
                    logger.info("There is record in the token_record table")
                    #select the newest record
                    query = select([token_record]).order_by(token_record.c.uuid.desc()).limit(1)
                    result = session.execute(query).fetchone()  # Fetch the result row
                    print("result="+str(result))
                    if result.amount_left < quantity:
                        logger.info("Quantity requested is more than the available token")
                        logger.info("inventory deduct fail .... rollbacking payment")
                        celery_app.send_task("create_payment", queue="q02", args=[payload, "rollback_payment"])
                        return "FAIL_INVENTORY (not enough token available)"
                    else:
                        logger.info("Quantity requested is possible")
                        logger.info("Updating record in the token_record table")
                        deduct_token(username, quantity, delivery, result.amount_left)
                        
                        delivery_task = celery_app.send_task("make_delivery", queue="q04", args=[payload, "make_delivery"])
                        #returning result to the order service
                        print("delivery_task.id="+str(delivery_task.id))
                        return waiting_delivery_result(delivery_task.id)
                        
            except Exception as e:
                logger.error("Error during database operation", exc_info=True)
            finally:
                session.close()
        elif fn == "rollback_inventory":
            rollback_inventory(username, quantity, delivery)
            print("rollback_inventory successfully")
            print("inventory service sending task to rollback payment")
            logger.info("Inventory service sending task to rollback payment")
            celery_app.send_task("rollback_payment", queue="q02", args=[payload, "rollback_payment"])
        else:
            logger.error("Invalid function name in inventory service")

@celery_app.task
def deduct_token(username: str, quantity: int, delivery: bool,amt_available: int):
    with tracer.start_as_current_span("commit_update_inventory"):
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
            logger.info("Inventory committed", extra={"username": username, "quantity": quantity, "amount_left": amt_available-quantity, "delivery": delivery})
            inventory_commit_counter.add(1)
            print("successfully deducted token and updated record in the token_record table")
        except Exception as e:
            logger.error("Error during database operation", exc_info=True)
        finally:
            session.close()

@celery_app.task
def waiting_delivery_result(delivery_task_id):
    with tracer.start_as_current_span("waiting_delivery_result"):
        time.sleep(0.5)
        delivery_task_result = AsyncResult(delivery_task_id)
        delivery_results_counter.add(1)
        if delivery_task_result.ready():
            result_value = delivery_task_result.result
            logger.info(f"Task result: {result_value}")
            return result_value
        else:
            logger.info("Delivery task is still running...")
            return "delivery task is still running..."

@celery_app.task
def rollback_inventory(username: str, quantity: int, delivery: bool):
    with tracer.start_as_current_span("rollback_inventory"):
        print("rollback_inventory")
        session = Session()
        try:
            #select the newest record
            query = select([token_record]).order_by(token_record.c.uuid.desc()).limit(1)
            result = session.execute(query).fetchone()  # Fetch the result row
            current_amt_available: int = result.amount_left
            logger.info("result_amt_left in rollback="+str(current_amt_available))
            new_amt_available: int = current_amt_available + quantity
            logger.info("new_amt_available in rollback="+str(new_amt_available))
            session.execute(
                insert(token_record).values(
                    amount_available=current_amt_available,
                    amount_taken=quantity,
                    amount_left=new_amt_available,
                    username=username,
                )
            )
            session.commit()
            inventory_rollback_counter.add(1)
            logger.info("Inventory rollback committed", extra={"username": username, "quantity": quantity, "delivery": delivery})
        except Exception as e:
            logger.error("Error during database operation", exc_info=True)
        finally:
            session.close()

        
    
