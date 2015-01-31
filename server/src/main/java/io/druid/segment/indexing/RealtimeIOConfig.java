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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.FirehoseFactory;
import io.druid.segment.realtime.plumber.PlumberSchool;

/**
 */
public class RealtimeIOConfig implements IOConfig
{
  private final FirehoseFactory firehoseFactory;
  private final PlumberSchool plumberSchool;

  @JsonCreator
  public RealtimeIOConfig(
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("plumber") PlumberSchool plumberSchool
  )
  {
    this.firehoseFactory = firehoseFactory;
    this.plumberSchool = plumberSchool;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  public PlumberSchool getPlumberSchool()
  {
    return plumberSchool;
  }
}
