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

package org.apache.druid.segment.serde;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class ComplexColumnSerializer<T> implements GenericColumnSerializer<T>
{
  @PublicApi
  public static ComplexColumnSerializer create(SegmentWriteOutMedium segmentWriteOutMedium, String filenameBase, ObjectStrategy strategy)
  {
    return new ComplexColumnSerializer(segmentWriteOutMedium, filenameBase, strategy);
  }

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ObjectStrategy strategy;
  private GenericIndexedWriter writer;

  private ComplexColumnSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String filenameBase, ObjectStrategy strategy)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    writer = new GenericIndexedWriter(segmentWriteOutMedium, StringUtils.format("%s.complex_column", filenameBase), strategy);
    writer.open();
  }

  @Override
  public void serialize(ColumnValueSelector<? extends T> selector) throws IOException
  {
    writer.write(selector.getObject());
  }

  @Override
  public long getSerializedSize()
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeToVersionOne(channel);
  }

  private void writeToVersionOne(WritableByteChannel channel) throws IOException
  {
    //null for the FileSmoosher means that we default to "version 1" of GenericIndexed.
    writer.writeTo(channel, null);
  }
}
