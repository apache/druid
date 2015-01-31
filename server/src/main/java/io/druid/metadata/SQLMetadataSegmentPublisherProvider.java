/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.realtime.SegmentPublisher;
import org.skife.jdbi.v2.IDBI;

import javax.validation.constraints.NotNull;

/**
 */
public class SQLMetadataSegmentPublisherProvider implements MetadataSegmentPublisherProvider
{
  @JacksonInject
  @NotNull
  private SQLMetadataConnector connector = null;

  @JacksonInject
  @NotNull
  private MetadataStorageTablesConfig config = null;

  @JacksonInject
  @NotNull
  private ObjectMapper jsonMapper = null;

  @Override
  public MetadataSegmentPublisher get()
  {
    return new SQLMetadataSegmentPublisher(jsonMapper, config, connector);
  }
}
