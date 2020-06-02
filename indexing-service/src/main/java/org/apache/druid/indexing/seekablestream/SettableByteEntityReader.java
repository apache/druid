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

package org.apache.druid.indexing.seekablestream;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.Transformer;
import org.apache.druid.segment.transform.TransformingInputEntityReader;

import java.io.File;
import java.io.IOException;

/**
 * A settable {@link InputEntityReader}. This class is intended to be used for only stream parsing in Kafka or Kinesis
 * indexing.
 */
class SettableByteEntityReader implements InputEntityReader
{
  private final InputFormat inputFormat;
  private final InputRowSchema inputRowSchema;
  private final Transformer transformer;
  private final File indexingTmpDir;

  private InputEntityReader delegate;

  SettableByteEntityReader(
      InputFormat inputFormat,
      InputRowSchema inputRowSchema,
      TransformSpec transformSpec,
      File indexingTmpDir
  )
  {
    this.inputFormat = Preconditions.checkNotNull(inputFormat, "inputFormat");
    this.inputRowSchema = inputRowSchema;
    this.transformer = transformSpec.toTransformer();
    this.indexingTmpDir = indexingTmpDir;
  }

  void setEntity(ByteEntity entity)
  {
    this.delegate = new TransformingInputEntityReader(
        // Yes, we are creating a new reader for every stream chunk.
        // This should be fine as long as initializing a reader is cheap which it is for now.
        inputFormat.createReader(inputRowSchema, entity, indexingTmpDir),
        transformer
    );
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return delegate.read();
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return delegate.sample();
  }
}
