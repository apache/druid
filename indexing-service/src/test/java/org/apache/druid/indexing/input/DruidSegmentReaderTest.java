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

package org.apache.druid.indexing.input;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class DruidSegmentReaderTest
{
  @Test
  public void testMakeCloseableIteratorFromSequenceAndSegmentFileCloseYielderOnClose() throws IOException
  {
    MutableBoolean isSequenceClosed = new MutableBoolean(false);
    MutableBoolean isFileClosed = new MutableBoolean(false);
    Sequence<Map<String, Object>> sequence = new BaseSequence<>(
        new IteratorMaker<Map<String, Object>, Iterator<Map<String, Object>>>()
        {
          @Override
          public Iterator<Map<String, Object>> make()
          {
            return Collections.emptyIterator();
          }

          @Override
          public void cleanup(Iterator<Map<String, Object>> iterFromMake)
          {
            isSequenceClosed.setValue(true);
          }
        }
    );
    CleanableFile cleanableFile = new CleanableFile()
    {
      @Override
      public File file()
      {
        return null;
      }

      @Override
      public void close()
      {
        isFileClosed.setValue(true);
      }
    };
    try (CloseableIterator<Map<String, Object>> iterator =
             DruidSegmentReader.makeCloseableIteratorFromSequenceAndSegmentFile(sequence, cleanableFile)) {
      while (iterator.hasNext()) {
        iterator.next();
      }
    }
    Assert.assertTrue("File is not closed", isFileClosed.booleanValue());
    Assert.assertTrue("Sequence is not closed", isSequenceClosed.booleanValue());
  }
}
