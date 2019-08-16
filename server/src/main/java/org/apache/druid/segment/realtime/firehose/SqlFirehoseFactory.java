/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.ResultSetException;
import org.skife.jdbi.v2.exceptions.StatementException;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlFirehoseFactory extends PrefetchSqlFirehoseFactory<String>
{
  @JsonProperty
  private final List<String> sqls;
  @Nullable
  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  private final ObjectMapper objectMapper;
  @JsonProperty
  private final SQLFirehoseDatabaseConnector sqlFirehoseDatabaseConnector;
  private final boolean foldCase;

  @JsonCreator
  public SqlFirehoseFactory(
      @JsonProperty("sqls") List<String> sqls,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("foldCase") boolean foldCase,
      @JsonProperty("database") SQLFirehoseDatabaseConnector sqlFirehoseDatabaseConnector,
      @JacksonInject @Smile ObjectMapper objectMapper
  )
  {
    super(
        maxCacheCapacityBytes,
        maxFetchCapacityBytes,
        prefetchTriggerBytes,
        fetchTimeout,
        objectMapper
    );
    Preconditions.checkArgument(sqls.size() > 0, "No SQL queries provided");

    this.sqls = sqls;
    this.objectMapper = objectMapper;
    this.sqlFirehoseDatabaseConnector = sqlFirehoseDatabaseConnector;
    this.foldCase = foldCase;
    this.connectorConfig = null;
  }

  @Override
  protected InputStream openObjectStream(String object, File fileName) throws IOException
  {
    Preconditions.checkNotNull(sqlFirehoseDatabaseConnector, "SQL Metadata Connector not configured!");
    try (FileOutputStream fos = new FileOutputStream(fileName)) {
      final JsonGenerator jg = objectMapper.getFactory().createGenerator(fos);
      sqlFirehoseDatabaseConnector.retryWithHandle(
          (handle) -> {
            ResultIterator<Map<String, Object>> resultIterator = handle.createQuery(
                object
            ).map(
                (index, r, ctx) -> {
                  Map<String, Object> resultRow = foldCase ? new CaseFoldedMap() : new HashMap<>();
                  ResultSetMetaData resultMetadata;
                  try {
                    resultMetadata = r.getMetaData();
                  }
                  catch (SQLException e) {
                    throw new ResultSetException("Unable to obtain metadata from result set", e, ctx);
                  }
                  try {
                    for (int i = 1; i <= resultMetadata.getColumnCount(); i++) {
                      String key = resultMetadata.getColumnName(i);
                      String alias = resultMetadata.getColumnLabel(i);
                      Object value = r.getObject(i);
                      resultRow.put(alias != null ? alias : key, value);
                    }
                  }
                  catch (SQLException e) {
                    throw new ResultSetException("Unable to access specific metadata from " +
                                                 "result set metadata", e, ctx);
                  }
                  return resultRow;
                }
            ).iterator();
            jg.writeStartArray();
            while (resultIterator.hasNext()) {
              jg.writeObject(resultIterator.next());
            }
            jg.writeEndArray();
            jg.close();
            return null;
          },
          (exception) -> {
            final boolean isStatementException = exception instanceof StatementException ||
                                                 (exception instanceof CallbackFailedException
                                                  && exception.getCause() instanceof StatementException);
            return sqlFirehoseDatabaseConnector.isTransientException(exception) && !(isStatementException);
          }
      );
    }
    return new FileInputStream(fileName);

  }

  private static class CaseFoldedMap extends HashMap<String, Object>
  {
    public static final long serialVersionUID = 1L;

    @Override
    public Object get(Object obj)
    {
      return super.get(StringUtils.toLowerCase((String) obj));
    }

    @Override
    public Object put(String key, Object value)
    {
      return super.put(StringUtils.toLowerCase(key), value);
    }

    @Override
    public boolean containsKey(Object obj)
    {
      return super.containsKey(StringUtils.toLowerCase((String) obj));
    }
  }

  @Override
  protected Collection<String> initObjects()
  {
    return sqls;
  }
}
