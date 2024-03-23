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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.TransformingInputEntityReader;

import java.io.File;
import java.io.IOException;

/**
 * A settable {@link InputEntityReader}. This class is intended to be used for only stream parsing in Kafka or Kinesis
 * indexing.
 */
class SettableByteEntityReader<T extends ByteEntity> implements InputEntityReader
{
  private final SettableByteEntity<T> entity;
  private final InputEntityReader delegate;

  SettableByteEntityReader(
      InputFormat inputFormat,
      InputRowSchema inputRowSchema,
      TransformSpec transformSpec,
      File indexingTmpDir
  )
  {
    Preconditions.checkNotNull(inputFormat, "inputFormat");
    final InputFormat format = JsonInputFormat.withLineSplittable(inputFormat, false);
    this.entity = new SettableByteEntity<>();
    this.delegate = new TransformingInputEntityReader(
        format.createReader(inputRowSchema, entity, indexingTmpDir),
        transformSpec.toTransformer()
    );
  }

  void setEntity(T entity)
  {
    this.entity.setEntity(entity);
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
