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

package org.apache.druid.msq.indexing;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.segment.RowAdapter;

import java.io.IOException;

public class CountableInputSourceReader implements InputSourceReader
{
  private final InputSourceReader inputSourceReader;
  private final ChannelCounters channelCounters;

  public CountableInputSourceReader(
      final InputSourceReader inputSourceReader,
      final ChannelCounters channelCounters
  )
  {
    this.inputSourceReader = inputSourceReader;
    this.channelCounters = channelCounters;
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
  {
    return inputSourceReader.read(inputStats).map(inputRow -> {
      channelCounters.incrementRowCount();
      return inputRow;
    });
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return inputSourceReader.sample();
  }

  @Override
  public RowAdapter<InputRow> rowAdapter()
  {
    return inputSourceReader.rowAdapter();
  }
}
