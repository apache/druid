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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FrontCodedIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteOrder;

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
      DictionaryWriter<String> writer;
      if (StringEncodingStrategy.FRONT_CODED.equals(encodingStrategy.getType())) {
        writer = new FrontCodedIndexedWriter<>(
            writeoutMedium,
            FrontCodedIndexedWriter.STRING_ENCODER,
            ByteOrder.nativeOrder(),
            ((StringEncodingStrategy.FrontCoded) encodingStrategy).getBucketSize()
        );
      } else {
        throw new ISE("Unknown encoding strategy: %s", encodingStrategy.getType());
      }
      return new EncodedStringDictionaryWriter(writer, encodingStrategy);
    }
  }
}
