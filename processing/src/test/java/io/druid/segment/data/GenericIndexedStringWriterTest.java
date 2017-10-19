/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import io.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GenericIndexedStringWriterTest
{
  @Test
  public void testRandomAccess() throws IOException
  {
    OnHeapMemorySegmentWriteOutMedium segmentWriteOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    GenericIndexedWriter<String> writer = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        "test",
        GenericIndexed.STRING_STRATEGY
    );
    writer.open();
    writer.write(null);
    List<String> strings = new ArrayList<>();
    strings.add(null);
    ThreadLocalRandom r = ThreadLocalRandom.current();
    for (int i = 0; i < 100_000; i++) {
      byte[] bs = new byte[r.nextInt(1, 10)];
      r.nextBytes(bs);
      String s = new String(bs, StandardCharsets.US_ASCII);
      strings.add(s);
      writer.write(s);
    }
    for (int i = 0; i < strings.size(); i++) {
      Assert.assertEquals(strings.get(i), writer.get(i));
    }
  }
}
