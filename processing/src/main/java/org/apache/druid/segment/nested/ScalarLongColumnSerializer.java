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

package org.apache.druid.segment.nested;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ColumnarLongsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Serializer for a {@link ScalarLongColumn}
 */
public class ScalarLongColumnSerializer extends ScalarNestedCommonFormatColumnSerializer<Long>
{
  private ColumnarLongsSerializer longsSerializer;

  public ScalarLongColumnSerializer(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      Closer closer
  )
  {
    super(name, indexSpec, segmentWriteOutMedium, closer);
  }

  @Override
  protected int processValue(@Nullable Object rawValue) throws IOException
  {
    final ExprEval<?> eval = ExprEval.bestEffortOf(rawValue).castTo(ExpressionType.LONG);

    final long val = eval.asLong();
    final int dictId = eval.isNumericNull() ? 0 : dictionaryIdLookup.lookupLong(val);
    longsSerializer.add(dictId == 0 ? 0L : val);
    return dictId;
  }

  @Override
  public void openDictionaryWriter() throws IOException
  {
    dictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.LONG.getStrategy(),
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    dictionaryWriter.open();
    dictionaryIdLookup = closer.register(
        new DictionaryIdLookup(
            name,
            FileUtils.getTempDir(),
            null,
            dictionaryWriter,
            null,
            null
        )
    );
  }

  @Override
  protected void openValueColumnSerializer() throws IOException
  {
    longsSerializer = CompressionFactory.getLongSerializer(
        name,
        segmentWriteOutMedium,
        StringUtils.format("%s.long_column", name),
        ByteOrder.nativeOrder(),
        indexSpec.getLongEncoding(),
        indexSpec.getDimensionCompression()
    );
    longsSerializer.open();
  }

  @Override
  public void serializeDictionaries(
      Iterable<String> strings,
      Iterable<Long> longs,
      Iterable<Double> doubles,
      Iterable<int[]> arrays
  ) throws IOException
  {
    if (dictionarySerialized) {
      throw new ISE("Long dictionary already serialized for column [%s], cannot serialize again", name);
    }

    // null is always 0
    dictionaryWriter.write(null);
    for (Long value : longs) {
      if (value == null) {
        continue;
      }
      dictionaryWriter.write(value);
    }
    dictionarySerialized = true;
  }

  @Override
  protected void writeValueColumn(FileSmoosher smoosher) throws IOException
  {
    writeInternal(smoosher, longsSerializer, LONG_VALUE_COLUMN_FILE_NAME);
  }

  @Override
  protected void writeDictionaryFile(FileSmoosher smoosher) throws IOException
  {
    if (dictionaryIdLookup.getLongBuffer() != null) {
      writeInternal(smoosher, dictionaryIdLookup.getLongBuffer(), LONG_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, dictionaryWriter, LONG_DICTIONARY_FILE_NAME);
    }
  }
}
