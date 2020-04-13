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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Creates firehose that produces data inlined in its own spec
 */
public class InlineFirehoseFactory implements FiniteFirehoseFactory<StringInputRowParser, String>
{
  private final String data;

  @VisibleForTesting
  @JsonCreator
  public InlineFirehoseFactory(@JsonProperty("data") String data)
  {
    this.data = Preconditions.checkNotNull(data, "data");
  }

  @JsonProperty
  public String getData()
  {
    return data;
  }

  @Override
  public Firehose connect(StringInputRowParser parser, @Nullable File temporaryDirectory) throws IOException
  {
    return new InlineFirehose(data, parser);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InlineFirehoseFactory factory = (InlineFirehoseFactory) o;
    return data.equals(factory.data);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(data);
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public Stream<InputSplit<String>> getSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    return Stream.of(new InputSplit<>(data));
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    return 1;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, String> withSplit(InputSplit<String> split)
  {
    return new InlineFirehoseFactory(split.get());
  }
}
