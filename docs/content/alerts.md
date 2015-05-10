---
layout: doc_page
---
# Druid Alerts

Druid generates alerts on getting into unexpected situations.

Alerts are emitted as JSON objects to a runtime log file or over HTTP (to a service such as Apache Kafka). Alert emission is disabled by default.

All Druid alerts share a common set of fields:

* `timestamp` - the time the alert was created
* `service` - the service name that emitted the alert
* `host` - the host name that emitted the alert
* `severity` - severity of the alert e.g. anomaly, component-failure, service-failure etc.
* `description` - a description of the alert
* `data` - if there was an exception then a JSON object with fields `exceptionType`, `exceptionMessage` and `exceptionStackTrace`
