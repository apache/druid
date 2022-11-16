package org.apache.druid.testing.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MsqSqlTasksWithTestQueries
{
  private final String sqlTask;
  private final String testQueriesFilePath;

  @JsonCreator
  public MsqSqlTasksWithTestQueries(
      @JsonProperty("sqlTask") String sqlTask,
      @JsonProperty("testQueriesFilePath") String testQueriesFilePath
  )
  {
    this.sqlTask = sqlTask;
    this.testQueriesFilePath = testQueriesFilePath;
  }

  @JsonProperty
  public String getSqlTask()
  {
    return this.sqlTask;
  }

  @JsonProperty
  public String getTestQueries()
  {
    return this.testQueriesFilePath;
  }

}
