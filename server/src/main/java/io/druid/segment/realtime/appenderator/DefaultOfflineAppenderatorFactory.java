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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;


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
  public Appenderator build(
      DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
  )
  {
    return Appenderators.createOffline(schema, config, metrics, dataSegmentPusher, objectMapper, indexIO, indexMerger);
  }
}
