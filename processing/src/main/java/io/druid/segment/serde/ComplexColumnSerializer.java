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

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class ComplexColumnSerializer implements GenericColumnSerializer
{
  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy strategy;
  private GenericIndexedWriter writer;
  public ComplexColumnSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy strategy
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
  }

  public static ComplexColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy strategy
  )
  {
    return new ComplexColumnSerializer(ioPeon, filenameBase, strategy);
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    writer = new GenericIndexedWriter(
        ioPeon, StringUtils.format("%s.complex_column", filenameBase), strategy
    );
    writer.open();
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void serialize(Object obj) throws IOException
  {
    writer.write(obj);
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public long getSerializedSize()
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeToChannelVersionOne(channel);
  }

  private void writeToChannelVersionOne(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(
        channel,
        null
    ); //null for the FileSmoosher means that we default to "version 1" of GenericIndexed.
  }
}
