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
package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Preconditions;
import io.druid.data.input.impl.prefetch.PrefetchSqlFirehoseFactory;
import io.druid.guice.annotations.Smile;
import io.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.ResultSetException;


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
  private final ObjectMapper objectMapper;
  private final SQLMetadataConnector sqlMetadataConnector;
  private final Boolean foldCase;

  @JsonCreator
  public SqlFirehoseFactory(
      @JsonProperty("sqls") List<String> sql,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("foldCase") Boolean foldCase,
      @JacksonInject SQLMetadataConnector sqlMetadataConnector,
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

    Preconditions.checkArgument(sql.size() > 0, "No SQL queries provided");

    this.sqls = sql;
    this.objectMapper = objectMapper;
    this.sqlMetadataConnector = sqlMetadataConnector;
    this.foldCase = (foldCase == null ? false : true);
  }

  @Override
  protected InputStream openObjectStream(String object, File fileName) throws IOException
  {
    Preconditions.checkNotNull(sqlMetadataConnector, "SQL Metadata Connector not configured!");
    try (FileOutputStream fos = new FileOutputStream(fileName)) {
      sqlMetadataConnector.retryWithHandle(
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
            while (resultIterator.hasNext()) {
              fos.write(objectMapper.writeValueAsBytes(resultIterator.next()));
            }
            return null;
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
      return super.get(((String) obj).toLowerCase());
    }

    @Override
    public Object put(String key, Object value)
    {
      return super.put(key.toLowerCase(), value);
    }

    @Override
    public boolean containsKey(Object obj)
    {
      return super.containsKey(((String) obj).toLowerCase());
    }
  }

  @Override
  protected Collection<String> initObjects()
  {
    return sqls;
  }
}
