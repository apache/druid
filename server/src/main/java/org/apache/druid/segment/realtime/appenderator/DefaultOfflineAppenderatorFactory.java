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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;


public class DefaultOfflineAppenderatorFactory implements AppenderatorFactory
{
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;

  @JsonCreator
  public DefaultOfflineAppenderatorFactory(
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject IndexIO indexIO,
      @JacksonInject IndexMerger indexMerger
  )
  {
    this.dataSegmentPusher = dataSegmentPusher;
    this.objectMapper = objectMapper;
    this.indexIO = indexIO;
    this.indexMerger = indexMerger;
  }

  @Override
  public Appenderator build(DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics)
  {
    return Appenderators.createOffline(
        schema.getDataSource(),
        schema,
        config,
        false,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger
    );
  }
}
