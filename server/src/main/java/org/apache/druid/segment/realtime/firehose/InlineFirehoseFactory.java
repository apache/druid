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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.impl.InlineFirehose;
import org.apache.druid.data.input.impl.StringInputRowParser;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;

/**
 * Creates firehose that produces data inlined in its own spec
 */
public class InlineFirehoseFactory implements FirehoseFactory<StringInputRowParser>
{
  @NotNull
  private final String data;

  @JsonCreator
  public InlineFirehoseFactory(@NotNull @JsonProperty("data") String data)
  {
    Preconditions.checkNotNull(data, "data");
    this.data = data;
  }

  @NotNull
  @JsonProperty
  public String getData()
  {
    return data;
  }

  @Override
  public Firehose connect(StringInputRowParser parser, File temporaryDirectory) throws IOException
  {
    return new InlineFirehose(data, parser);
  }
}
