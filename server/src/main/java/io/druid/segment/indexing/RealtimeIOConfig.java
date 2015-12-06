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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.segment.realtime.plumber.PlumberSchool;

/**
 */
public class RealtimeIOConfig implements IOConfig
{
  private final FirehoseFactory firehoseFactory;
  private final PlumberSchool plumberSchool;
  private final FirehoseFactoryV2 firehoseFactoryV2;

  @JsonCreator
  public RealtimeIOConfig(
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("plumber") PlumberSchool plumberSchool,
      @JsonProperty("firehoseV2") FirehoseFactoryV2 firehoseFactoryV2
  )
  {
    if (firehoseFactory != null && firehoseFactoryV2 != null) {
      throw new IllegalArgumentException("Only provide one of firehose or firehoseV2");
    }

    this.firehoseFactory = firehoseFactory;
    this.plumberSchool = plumberSchool;
    this.firehoseFactoryV2 = firehoseFactoryV2;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @JsonProperty("firehoseV2")
  public FirehoseFactoryV2 getFirehoseFactoryV2()
  {
    return firehoseFactoryV2;
  }

  public PlumberSchool getPlumberSchool()
  {
    return plumberSchool;
  }
}
