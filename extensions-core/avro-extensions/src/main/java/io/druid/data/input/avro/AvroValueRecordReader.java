/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroRecordReaderBase;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class AvroValueRecordReader extends AvroRecordReaderBase<NullWritable, GenericRecord, GenericRecord>
{
  public AvroValueRecordReader(Schema readerSchema)
  {
    super(readerSchema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException
  {
    return NullWritable.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GenericRecord getCurrentValue() throws IOException, InterruptedException
  {
    return getCurrentRecord();
  }
}
