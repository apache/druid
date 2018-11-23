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

package org.apache.druid.data.input.impl.prefetch;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonIteratorTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper(new SmileFactory());
    List<Map<String, Object>> expectedList = ImmutableList.of(ImmutableMap.of("key1", "value1", "key2", 2));
    File testFile = File.createTempFile("testfile", "");
    TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>()
    {
    };
    try (FileOutputStream fos = new FileOutputStream(testFile)) {
      final JsonGenerator jg = mapper.getFactory().createGenerator(fos);
      jg.writeStartArray();
      for (Map<String, Object> mapFromList : expectedList) {
        jg.writeObject(mapFromList);
      }
      jg.writeEndArray();
      jg.close();
    }

    JsonIterator<Map<String, Object>> testJsonIterator = new JsonIterator<>(type, new FileInputStream(testFile), () -> {
    }, mapper);
    List<Map<String, Object>> actualList = new ArrayList<>();
    while (testJsonIterator.hasNext()) {
      actualList.add(testJsonIterator.next());
    }
    testJsonIterator.close();

    Assert.assertEquals(expectedList, actualList);
  }
}
