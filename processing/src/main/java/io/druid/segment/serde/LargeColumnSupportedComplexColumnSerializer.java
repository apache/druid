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

package io.druid.segment.serde;

import io.druid.guice.annotations.PublicApi;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class LargeColumnSupportedComplexColumnSerializer implements GenericColumnSerializer
{
  @PublicApi
  public static LargeColumnSupportedComplexColumnSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy
  )
  {
    return new LargeColumnSupportedComplexColumnSerializer(segmentWriteOutMedium, filenameBase, strategy);
  }

  public static LargeColumnSupportedComplexColumnSerializer createWithColumnSize(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy,
      int columnSize
  )
  {
    return new LargeColumnSupportedComplexColumnSerializer(segmentWriteOutMedium, filenameBase, strategy, columnSize);
  }

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ObjectStrategy strategy;
  private final int columnSize;
  private GenericIndexedWriter writer;

  private LargeColumnSupportedComplexColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy
  )
  {
    this(segmentWriteOutMedium, filenameBase, strategy, Integer.MAX_VALUE);
  }

  private LargeColumnSupportedComplexColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy,
      int columnSize
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.columnSize = columnSize;
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    writer = new GenericIndexedWriter(
        segmentWriteOutMedium,
        StringUtils.format("%s.complex_column", filenameBase),
        strategy,
        columnSize
    );
    writer.open();
  }

  @Override
  public void serialize(ColumnValueSelector selector) throws IOException
  {
    writer.write(selector.getObject());
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writer.writeTo(channel, smoosher);
  }

}
