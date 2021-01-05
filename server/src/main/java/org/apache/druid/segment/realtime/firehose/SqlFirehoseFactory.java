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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.metadata.input.SqlEntity;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
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
    this.sqlFirehoseDatabaseConnector = Preconditions.checkNotNull(
        sqlFirehoseDatabaseConnector,
        "SQL Metadata Connector not configured!"
    );
    this.foldCase = foldCase;
    this.connectorConfig = null;
  }

  @Override
  protected InputStream openObjectStream(String sql, File fileName) throws IOException
  {
    SqlEntity.openCleanableFile(sql, sqlFirehoseDatabaseConnector, objectMapper, foldCase, fileName);
    return new FileInputStream(fileName);
  }

  @Override
  protected Collection<String> initObjects()
  {
    return sqls;
  }

  @Override
  public FiniteFirehoseFactory<InputRowParser<Map<String, Object>>, String> withSplit(InputSplit<String> split)
  {
    return new SqlFirehoseFactory(
        Collections.singletonList(split.get()),
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        foldCase,
        sqlFirehoseDatabaseConnector,
        objectMapper
    );
  }
}
