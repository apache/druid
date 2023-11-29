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
import org.apache.druid.segment.data.ColumnarDoublesSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Serializer for a {@link ScalarDoubleColumn}
 */
public class ScalarDoubleColumnSerializer extends ScalarNestedCommonFormatColumnSerializer<Double>
{
  private ColumnarDoublesSerializer doublesSerializer;

  public ScalarDoubleColumnSerializer(
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
    final ExprEval<?> eval = ExprEval.bestEffortOf(rawValue).castTo(ExpressionType.DOUBLE);
    final double val = eval.asDouble();
    final int dictId = eval.isNumericNull() ? 0 : dictionaryIdLookup.lookupDouble(val);
    doublesSerializer.add(dictId == 0 ? 0.0 : val);
    return dictId;
  }

  @Override
  public void openDictionaryWriter() throws IOException
  {
    dictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.DOUBLE.getStrategy(),
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
            null,
            dictionaryWriter,
            null
        )
    );
  }

  @Override
  protected void openValueColumnSerializer() throws IOException
  {
    doublesSerializer = CompressionFactory.getDoubleSerializer(
        name,
        segmentWriteOutMedium,
        StringUtils.format("%s.double_column", name),
        ByteOrder.nativeOrder(),
        indexSpec.getDimensionCompression()
    );
    doublesSerializer.open();
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
      throw new ISE("Double dictionary already serialized for column [%s], cannot serialize again", name);
    }

    // null is always 0
    dictionaryWriter.write(null);

    for (Double value : doubles) {
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
    writeInternal(smoosher, doublesSerializer, DOUBLE_VALUE_COLUMN_FILE_NAME);
  }

  @Override
  protected void writeDictionaryFile(FileSmoosher smoosher) throws IOException
  {
    if (dictionaryIdLookup.getDoubleBuffer() != null) {
      writeInternal(smoosher, dictionaryIdLookup.getDoubleBuffer(), DOUBLE_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, dictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    }
  }
}
