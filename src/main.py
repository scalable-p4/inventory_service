from fastapi import FastAPI
from src.database import database
from src.router import router as router
from starlette.middleware.cors import CORSMiddleware
from src.config import app_configs, settings
import aioredis
import logging
import os
# OpenTelemetry imports for tracing and metrics
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# Logging imports
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

service_name = "inventory_app"
OTEL_ENDPOINT = os.getenv('OTEL_ENDPOINT', "otel-collector:4317")

# Initialize TracerProvider for OTLP
resource = Resource(attributes={SERVICE_NAME: service_name})
trace_provider = TracerProvider(resource=resource)
otlp_trace_exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
trace_provider.add_span_processor(BatchSpanProcessor(otlp_trace_exporter))
trace.set_tracer_provider(trace_provider)

# Initialize MeterProvider for OTLP
metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True))
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

# Initialize LoggerProvider for OTLP
logger_provider = LoggerProvider(resource=resource)
otlp_log_exporter = OTLPLogExporter(endpoint=OTEL_ENDPOINT, insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

# Attach OTLP handler to root logger
logging.getLogger().addHandler(handler)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_origin_regex=settings.CORS_ORIGINS_REGEX,
    allow_credentials=True,
    allow_methods=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
    allow_headers=settings.CORS_HEADERS,
)

FastAPIInstrumentor.instrument_app(app)

tracer = trace.get_tracer(__name__)

# Test counter metric
meter = metrics.get_meter(__name__)
request_counter = meter.create_counter(
    name="requests_order_counter",
    description="Counts total number of requests",
    unit="1",
)

@app.on_event("startup")
async def startup() -> None:
    await database.connect()

@app.get("/")
async def root():
    with tracer.start_as_current_span("request_order"):
        request_counter.add(1)
        logging.info("Received a request at root endpoint")
        return {"message": "Hello World"}

app.include_router(router, prefix="/api", tags=["API"])

@app.on_event("shutdown")
def shutdown() -> None:
    logger_provider.shutdown()
