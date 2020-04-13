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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * Firehose to give out only first n events from the delegate firehose.
 */
public class FixedCountFirehoseFactory implements FirehoseFactory
{
  private final FirehoseFactory delegate;
  private final int count;

  @JsonCreator
  public FixedCountFirehoseFactory(
      @JsonProperty("delegate") FirehoseFactory delegate,
      @JsonProperty("count") int count
  )
  {
    this.delegate = delegate;
    this.count = count;
  }

  @JsonProperty
  public FirehoseFactory getDelegate()
  {
    return delegate;
  }

  @JsonProperty
  public int getCount()
  {
    return count;
  }

  @Override
  public Firehose connect(final InputRowParser parser, File temporaryDirectory) throws IOException
  {
    return new Firehose()
    {
      private int i = 0;
      private final Firehose delegateFirehose = delegate.connect(parser, temporaryDirectory);

      @Override
      public boolean hasMore() throws IOException
      {
        return i < count && delegateFirehose.hasMore();
      }

      @Nullable
      @Override
      public InputRow nextRow() throws IOException
      {
        Preconditions.checkArgument(i++ < count, "Max events limit reached.");
        return delegateFirehose.nextRow();
      }

      @Override
      public void close() throws IOException
      {
        delegateFirehose.close();
      }
    };
  }
}
