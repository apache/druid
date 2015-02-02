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

package io.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.db.DbTablesConfig;
import io.druid.segment.realtime.DbSegmentPublisher;
import io.druid.segment.realtime.SegmentPublisher;
import org.skife.jdbi.v2.IDBI;

import javax.validation.constraints.NotNull;

/**
 */
public class DbSegmentPublisherProvider implements SegmentPublisherProvider
{
  @JacksonInject
  @NotNull
  private IDBI idbi = null;

  @JacksonInject
  @NotNull
  private DbTablesConfig config = null;

  @JacksonInject
  @NotNull
  private ObjectMapper jsonMapper = null;

  @Override
  public SegmentPublisher get()
  {
    return new DbSegmentPublisher(jsonMapper, config, idbi);
  }
}
