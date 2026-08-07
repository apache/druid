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
import org.apache.druid.segment.AutoTypeColumnMerger;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;

public class DictionaryIdLookupTest extends InitializedNullHandlingTest
{
  @TempDir
  private Path tempDir;

  @Test
  public void testIdLookup() throws IOException
  {
    // add some values
    ValueDictionary dictionary = new ValueDictionary();
    dictionary.addStringValue("hello");
    dictionary.addStringValue("world");
    dictionary.addStringValue(null);
    dictionary.addLongValue(123L);
    dictionary.addLongValue(-123L);
    dictionary.addDoubleValue(1.234);
    dictionary.addDoubleValue(0.001);
    dictionary.addStringArray(new Object[]{"hello", "world"});
    dictionary.addLongArray(new Object[]{1L, 2L, 3L});
    dictionary.addDoubleArray(new Object[]{0.01, -1.234, 0.001, 1.234});

    // sort them
    SortedValueDictionary sortedValueDictionary = dictionary.getSortedCollector();

    // setup dictionary writers
    SegmentWriteOutMedium medium = TmpFileSegmentWriteOutMediumFactory.instance()
                                                                      .makeSegmentWriteOutMedium(FileUtils.createTempDirInLocation(tempDir, "medium"));
    DictionaryWriter<String> stringWriter = StringEncodingStrategies.getStringDictionaryWriter(
        new StringEncodingStrategy.FrontCoded(4, (byte) 1),
        medium,
        "test"
    );
    FixedIndexedWriter<Long> longWriter = new FixedIndexedWriter<>(
        medium,
        TypeStrategies.LONG,
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    FixedIndexedWriter<Double> doubleWriter = new FixedIndexedWriter<>(
        medium,
        TypeStrategies.DOUBLE,
        ByteOrder.nativeOrder(),
        Double.BYTES,
        true
    );
    FrontCodedIntArrayIndexedWriter arrayWriter = new FrontCodedIntArrayIndexedWriter(
        medium,
        ByteOrder.nativeOrder(),
        4
    );

    File dictTempDir = FileUtils.createTempDirInLocation(tempDir, "dict");

    // make lookup with references to writers
    DictionaryIdLookup idLookup = new DictionaryIdLookup(
        "test",
        dictTempDir,
        stringWriter,
        longWriter,
        doubleWriter,
        arrayWriter
    );

    // write the stuff
    stringWriter.open();
    longWriter.open();
    doubleWriter.open();
    arrayWriter.open();

    File tempDir = dictTempDir;
    Assertions.assertEquals(0, tempDir.listFiles().length);

    for (String s : sortedValueDictionary.getSortedStrings()) {
      stringWriter.write(s);
    }
    for (Long l : sortedValueDictionary.getSortedLongs()) {
      longWriter.write(l);
    }
    for (Double d : sortedValueDictionary.getSortedDoubles()) {
      doubleWriter.write(d);
    }

    Iterable<int[]> sortedArrays = () -> new AutoTypeColumnMerger.ArrayDictionaryMergingIterator(
        new Iterable[]{sortedValueDictionary.getSortedArrays()},
        idLookup
    );

    Assertions.assertEquals(0, tempDir.listFiles().length);

    // looking up some values pulls in string dictionary and long dictionary
    Assertions.assertEquals(0, idLookup.lookupString(null));
    Assertions.assertEquals(1, idLookup.lookupString("hello"));
    Assertions.assertEquals(2, idLookup.lookupString("world"));
    Assertions.assertEquals(3, idLookup.lookupLong(-123L));

    Assertions.assertEquals(2, tempDir.listFiles().length);

    // writing arrays needs to use the lookups for lower value dictionaries, so will create string, long, and double
    // temp dictionary files
    for (int[] arr : sortedArrays) {
      arrayWriter.write(arr);
    }
    Assertions.assertEquals(3, tempDir.listFiles().length);

    Assertions.assertEquals(8, idLookup.lookupDouble(-1.234));
    Assertions.assertEquals(11, idLookup.lookupDouble(1.234));

    Assertions.assertEquals(3, tempDir.listFiles().length);

    // looking up arrays pulls in array file
    Assertions.assertEquals(12, idLookup.lookupArray(new int[]{1, 2}));
    Assertions.assertEquals(13, idLookup.lookupArray(new int[]{4, 5, 6}));
    Assertions.assertEquals(14, idLookup.lookupArray(new int[]{10, 8, 9, 11}));
    Assertions.assertEquals(4, tempDir.listFiles().length);

    // close it removes all the temp files
    idLookup.close();
    Assertions.assertEquals(0, tempDir.listFiles().length);
  }
}
