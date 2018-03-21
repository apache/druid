/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.history;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Objects;

public class QueryHistoryEntry
{
  public static final String CTX_SQL_QUERY_TEXT = "sqlQueryText";

  public static final String TYPE_BROKER_TIME = "broker_time";
  public static final String TYPE_NODE_TIME = "node_time";
  public static final String TYPE_SQL_QUERY_TEXT = "sql_query_text";
  public static final String TYPE_DATASOURCES = "datasources";

  private final String queryID;
  private final String type;
  private final String payload;
  private final DateTime createdDate;



  private QueryHistoryEntry(
      String queryID,
      String type,
      String payload
  )
  {
    this(queryID, type, payload, DateTimes.nowUtc());
  }

  @JsonCreator
  public QueryHistoryEntry(
      @JsonProperty("queryID") String queryID,
      @JsonProperty("type") String type,
      @JsonProperty("payload") String payload,
      @JsonProperty("createdDate") DateTime createdDate
  )
  {
    this.queryID = queryID;
    this.type = type;
    this.payload = payload;
    this.createdDate = createdDate;
  }


  @JsonProperty
  public String getQueryID()
  {
    return queryID;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public String getPayload()
  {
    return payload;
  }

  @JsonProperty
  public DateTime getCreatedDate()
  {
    return createdDate;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryHistoryEntry that = (QueryHistoryEntry) o;
    return Objects.equals(queryID, that.queryID) &&
        Objects.equals(type, that.type) &&
        Objects.equals(payload, that.payload) &&
        Objects.equals(createdDate, that.createdDate);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(queryID, type, payload, createdDate);
  }

  @Override
  public String toString()
  {
    return "QueryHistoryEntry{" +
        "queryID='" + queryID + '\'' +
        ", type='" + type + '\'' +
        ", payload='" + payload + '\'' +
        ", createdDate=" + createdDate +
        '}';
  }

  public static class Builder
  {
    private String queryID;
    private String type;
    private String payload;

    private Builder()
    {
      this.queryID = null;
      this.type = null;
      this.payload = null;
    }

    public Builder queryID(String queryID)
    {
      this.queryID = queryID;
      return this;
    }

    public Builder type(String type)
    {
      this.type = type;
      return this;
    }

    public Builder payload(String payload)
    {
      this.payload = payload;
      return this;
    }

    public QueryHistoryEntry build()
    {
      return new QueryHistoryEntry(queryID, type, payload);
    }
  }
}
