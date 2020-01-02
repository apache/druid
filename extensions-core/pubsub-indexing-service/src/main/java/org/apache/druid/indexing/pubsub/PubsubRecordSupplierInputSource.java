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

package org.apache.druid.indexing.pubsub;

import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;

/**
 * {@link org.apache.druid.data.input.InputSource} wrapping {@link RecordSupplier}. It will fetch data via
 * RecordSupplier and convert it into {@link ByteEntity}. See {@link #createEntityIterator}.
 */
public class PubsubRecordSupplierInputSource extends AbstractInputSource
{
  private final String topic;
  private final PubsubRecordSupplier recordSupplier;

  PubsubRecordSupplierInputSource(
      String topic,
      PubsubRecordSupplier recordSupplier
  )
  {
    this.topic = topic;
    this.recordSupplier = recordSupplier;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        createEntityIterator(),
        temporaryDirectory
    );
  }

  /**
   * Returns an iterator converting each byte array from RecordSupplier into a ByteEntity. Note that the
   * returned iterator will be blocked until the RecordSupplier gives any data.
   */
  CloseableIterator<InputEntity> createEntityIterator()
  {
    return new CloseableIterator<InputEntity>()
    {
      private Iterator<Object> recordIterator;
      private Iterator<byte[]> bytesIterator;
      private volatile boolean closed;

      @Override
      public boolean hasNext()
      {
        return bytesIterator != null && bytesIterator.hasNext();
      }

      @Override
      public InputEntity next()
      {
        return new ByteEntity(bytesIterator.next());
      }

      @Override
      public void close()
      {
        closed = true;
        recordSupplier.close();
      }
    };
  }
}
