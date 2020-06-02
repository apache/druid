This package contains ingestion specs for datasources used by Calcite Tests.

The purpose of these files is to make it easier to look at and manipulate the data under test so that you can easily
validate if the results of the SQL query written are as expected. 

> NOTE: The provided specs are not guaranteed to be in sync with the datasources used by the test. The source of truth
> is org.apache.druid.sql.calcite.util.CalciteTests
