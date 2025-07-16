# Testcontainers for Apache Druid

This module contains Testcontainers implementation for running Apache Druid services.
The dependencies in the pom must be minimal so that this module can be used independently by libraries and codebases outside Druid to run Testcontainers.

## Usage

```
final DruidContainer container = new DruidContainer("overlord", "apache/druid:33.0.0");
container.start();
```

## Standard Druid Images

1. `apache/druid:33.0.0`
2. `apache/druid:32.0.1`
3. `apache/druid:31.0.2`

## Next steps

- Add Druid command for running a starter cluster in a single container
- Publish this extension as a community module for Testcontainers
- Contribute the code to the main Testcontainers repository
