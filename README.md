# Robinhood Backend
- This project entails the development of a microservices architecture consisting of four backend services, designed with a Saga pattern for handling distributed transactions and ensuring data consistency. It incorporates OpenTelemetry for comprehensive observability across the services. For logging, the project integrates with Loki, offering efficient log aggregation and querying. Metrics monitoring is managed through Prometheus, allowing for data collection. Tracing of requests across the microservices is achieved via Jaeger, which assists in performance analysis and troubleshooting. Finally, the project leverages Grafana for the visualization of logs, metrics, and traces, providing a unified dashboard for monitoring
## Authors

- Thana Lertlum-umpaiwong 6380271

- Tanapon Techathammanun 6281332


## All service repos
    1. (https://github.com/scalable-p4/order_service)
    2. (https://github.com/scalable-p4/payment_service)
    3. (https://github.com/scalable-p4/inventory_service)
    4. (https://github.com/scalable-p4/delivery_service)

    - the first one is order_service
    - the second one is payment_service
    - the third one is inventory service
    - the fourth one is delivery service
## How to run
    1. Clone every related repos
    2. you can create a folder and clone each one into the folder
    4. go to k8s folder in order_service then configure the configmap and run kubectl apply -f .
    5. Finish!!!
