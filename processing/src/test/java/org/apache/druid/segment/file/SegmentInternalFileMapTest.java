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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SegmentInternalFileMapTest
{
  private static final SegmentInternalFileMetadata A = new SegmentInternalFileMetadata(0, 0, 10);
  private static final SegmentInternalFileMetadata B = new SegmentInternalFileMetadata(1, 5L + Integer.MAX_VALUE, 20);
  private static final SegmentInternalFileMetadata C = new SegmentInternalFileMetadata(0, 10, 0);

  @Test
  public void testCopyOf()
  {
    final SegmentInternalFileMap files = SegmentInternalFileMap.copyOf(makeSource());

    Assertions.assertEquals(3, files.size());
    Assertions.assertEquals(List.of("__base/a", "__base/b", "c"), new ArrayList<>(files.keySet()));
    Assertions.assertEquals(makeSource(), files);
    Assertions.assertEquals(files, makeSource());
    Assertions.assertEquals(makeSource().hashCode(), files.hashCode());
    Assertions.assertSame(files, SegmentInternalFileMap.copyOf(files));
  }

  @Test
  public void testLookups()
  {
    final SegmentInternalFileMap files = SegmentInternalFileMap.copyOf(makeSource());

    Assertions.assertEquals(B, files.get("__base/b"));
    Assertions.assertNull(files.get("__base"));
    Assertions.assertNull(files.get(1));
    Assertions.assertTrue(files.containsKey("c"));
    Assertions.assertFalse(files.containsKey("d"));
    Assertions.assertTrue(files.keySet().contains("c"));
    Assertions.assertFalse(files.keySet().contains("d"));

    final int index = files.indexOf("__base/b");
    Assertions.assertEquals(1, index);
    Assertions.assertEquals("__base/b", files.getName(index));
    Assertions.assertEquals(1, files.getContainer(index));
    Assertions.assertEquals(5L + Integer.MAX_VALUE, files.getStartOffset(index));
    Assertions.assertEquals(20, files.getSize(index));
    Assertions.assertTrue(files.indexOf("d") < 0);
  }

  @Test
  public void testImmutable()
  {
    final SegmentInternalFileMap files = SegmentInternalFileMap.copyOf(makeSource());

    Assertions.assertThrows(UnsupportedOperationException.class, () -> files.put("d", A));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> files.remove("c"));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> files.keySet().remove("c"));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> files.entrySet().clear());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.JSON_MAPPER;
    final SegmentInternalFileMap files = SegmentInternalFileMap.copyOf(makeSource());

    final Map<String, SegmentInternalFileMetadata> roundTrip = mapper.readValue(
        mapper.writeValueAsString(files),
        new TypeReference<>() {}
    );
    Assertions.assertEquals(makeSource(), roundTrip);
  }

  @Test
  public void testMetadataEquals()
  {
    EqualsVerifier.forClass(SegmentInternalFileMetadata.class).usingGetClass().verify();
  }

  private static Map<String, SegmentInternalFileMetadata> makeSource()
  {
    final Map<String, SegmentInternalFileMetadata> source = new HashMap<>();
    source.put("c", C);
    source.put("__base/b", B);
    source.put("__base/a", A);
    return source;
  }
}
