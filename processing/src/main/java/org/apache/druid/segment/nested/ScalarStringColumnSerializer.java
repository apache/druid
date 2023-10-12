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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Serializer for a string {@link NestedCommonFormatColumn} that can be read with
 * {@link StringUtf8DictionaryEncodedColumn}.
 */
public class ScalarStringColumnSerializer extends ScalarNestedCommonFormatColumnSerializer<String>
{
  public ScalarStringColumnSerializer(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      Closer closer
  )
  {
    super(name, indexSpec, segmentWriteOutMedium, closer);
  }

  @Override
  protected int processValue(@Nullable Object rawValue)
  {
    final ExprEval<?> eval = ExprEval.bestEffortOf(rawValue);
    final String s = eval.castTo(ExpressionType.STRING).asString();
    final int dictId = dictionaryIdLookup.lookupString(s);
    return dictId;
  }

  @Override
  public void openDictionaryWriter() throws IOException
  {
    dictionaryWriter = StringEncodingStrategies.getStringDictionaryWriter(
        indexSpec.getStringDictionaryEncoding(),
        segmentWriteOutMedium,
        name
    );
    dictionaryWriter.open();
    dictionaryIdLookup = closer.register(
        new DictionaryIdLookup(
            name,
            dictionaryWriter,
            null,
            null,
            null
        )
    );
  }

  @Override
  protected void openValueColumnSerializer()
  {
    // no extra value column for strings
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
      throw new ISE("String dictionary already serialized for column [%s], cannot serialize again", name);
    }

    // null is always 0
    dictionaryWriter.write(null);
    for (String value : strings) {
      value = NullHandling.emptyToNullIfNeeded(value);
      if (value == null) {
        continue;
      }

      dictionaryWriter.write(value);
    }
    dictionarySerialized = true;
  }

  @Override
  protected void writeValueColumn(FileSmoosher smoosher)
  {
    // no extra value column for strings
  }

  @Override
  protected void writeDictionaryFile(FileSmoosher smoosher) throws IOException
  {
    if (dictionaryIdLookup.getStringBufferMapper() != null) {
      SmooshedFileMapper fileMapper = dictionaryIdLookup.getStringBufferMapper();
      for (String name : fileMapper.getInternalFilenames()) {
        smoosher.add(name, fileMapper.mapFile(name));
      }
    } else {
      writeInternal(smoosher, dictionaryWriter, STRING_DICTIONARY_FILE_NAME);
    }
  }
}
