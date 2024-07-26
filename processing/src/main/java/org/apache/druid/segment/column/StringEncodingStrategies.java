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

package org.apache.druid.segment.column;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.FrontCodedIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

public class StringEncodingStrategies
{
  public static DictionaryWriter<String> getStringDictionaryWriter(
      StringEncodingStrategy encodingStrategy,
      SegmentWriteOutMedium writeoutMedium,
      String fileName
  )
  {
    // write plain utf8 in the legacy format, where generic indexed was written directly
    if (StringEncodingStrategy.UTF8.equals(encodingStrategy.getType())) {
      return new GenericIndexedWriter<>(writeoutMedium, fileName, GenericIndexed.STRING_STRATEGY);
    } else {
      // otherwise, we wrap in an EncodedStringDictionaryWriter so that we write a small header that includes
      // a version byte that should hopefully never conflict with a GenericIndexed version, along with a byte
      // from StringEncodingStrategy.getId to indicate which encoding strategy is used for the dictionary before
      // writing the dictionary itself
      DictionaryWriter<byte[]> writer;
      if (StringEncodingStrategy.FRONT_CODED.equals(encodingStrategy.getType())) {
        StringEncodingStrategy.FrontCoded strategy = (StringEncodingStrategy.FrontCoded) encodingStrategy;
        writer = new FrontCodedIndexedWriter(
            writeoutMedium,
            IndexIO.BYTE_ORDER,
            strategy.getBucketSize(),
            strategy.getFormatVersion()
        );
      } else {
        throw new ISE("Unknown encoding strategy: %s", encodingStrategy.getType());
      }
      return new EncodedStringDictionaryWriter(writer, encodingStrategy);
    }
  }

  public static Supplier<? extends Indexed<ByteBuffer>> getStringDictionarySupplier(
      SmooshedFileMapper mapper,
      ByteBuffer stringDictionaryBuffer,
      ByteOrder byteOrder
  )
  {
    final int dictionaryStartPosition = stringDictionaryBuffer.position();
    final byte dictionaryVersion = stringDictionaryBuffer.get();

    if (dictionaryVersion == EncodedStringDictionaryWriter.VERSION) {
      final byte encodingId = stringDictionaryBuffer.get();
      if (encodingId == StringEncodingStrategy.FRONT_CODED_ID) {
        return FrontCodedIndexed.read(
            stringDictionaryBuffer,
            byteOrder
        );
      } else if (encodingId == StringEncodingStrategy.UTF8_ID) {
        // this cannot happen naturally right now since generic indexed is written in the 'legacy' format, but
        // this provides backwards compatibility should we switch at some point in the future to always
        // writing dictionaryVersion
        return GenericIndexed.read(stringDictionaryBuffer, GenericIndexed.UTF8_STRATEGY, mapper)::singleThreaded;
      } else {
        throw new ISE("impossible, unknown encoding strategy id: %s", encodingId);
      }
    } else {
      // legacy format that only supports plain utf8 enoding stored in GenericIndexed and the byte we are reading
      // as dictionaryVersion is actually also the GenericIndexed version, so we reset start position so the
      // GenericIndexed version can be correctly read
      stringDictionaryBuffer.position(dictionaryStartPosition);
      return GenericIndexed.read(stringDictionaryBuffer, GenericIndexed.UTF8_STRATEGY, mapper)::singleThreaded;
    }
  }

  /**
   * Adapter to convert {@link Indexed<ByteBuffer>} with utf8 encoded bytes into {@link Indexed<String>} to be friendly
   * to consumers.
   */
  public static final class Utf8ToStringIndexed implements Indexed<String>
  {
    private final Indexed<ByteBuffer> delegate;

    public Utf8ToStringIndexed(Indexed<ByteBuffer> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Nullable
    @Override
    public String get(int index)
    {
      return StringUtils.fromUtf8Nullable(delegate.get(index));
    }

    @Override
    public int indexOf(@Nullable String value)
    {
      return delegate.indexOf(StringUtils.toUtf8ByteBuffer(value));
    }

    @Override
    public Iterator<String> iterator()
    {
      final Iterator<ByteBuffer> delegateIterator = delegate.iterator();
      return new Iterator<String>()
      {
        @Override
        public boolean hasNext()
        {
          return delegateIterator.hasNext();
        }

        @Override
        public String next()
        {
          return StringUtils.fromUtf8Nullable(delegateIterator.next());
        }
      };
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("delegateIndex", delegate);
    }
  }
}
