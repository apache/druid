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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class BoundedStreamConfigTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(BoundedStreamConfig.class)
                  .withNonnullFields("startSequenceNumbers", "endSequenceNumbers")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testConstructorWithNullStartSequenceNumbers()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(null, Map.of("0", 500L))
    );
    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithNullEndSequenceNumbers()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(Map.of("0", 100L), null)
    );
    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithEmptyStartSequenceNumbers()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(Map.of(), Map.of("0", 500L))
    );
    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithEmptyEndSequenceNumbers()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(Map.of("0", 100L), Map.of())
    );
    Assert.assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testConstructorWithMismatchedPartitions()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new BoundedStreamConfig(Map.of("0", 100L), Map.of("1", 500L))
    );
    Assert.assertTrue(ex.getMessage().contains("must have matching partition sets"));
  }

  @Test
  public void testSerializationDeserialization() throws Exception
  {
    BoundedStreamConfig config = new BoundedStreamConfig(
        Map.of("0", 100, "1", 200),
        Map.of("0", 500, "1", 600)
    );

    BoundedStreamConfig deserialized = mapper.readValue(
        mapper.writeValueAsString(config),
        BoundedStreamConfig.class
    );

    Assert.assertEquals(2, deserialized.getStartSequenceNumbers().size());
    Assert.assertEquals(2, deserialized.getEndSequenceNumbers().size());
    Assert.assertEquals(100, deserialized.getStartSequenceNumbers().get("0"));
    Assert.assertEquals(200, deserialized.getStartSequenceNumbers().get("1"));
    Assert.assertEquals(500, deserialized.getEndSequenceNumbers().get("0"));
    Assert.assertEquals(600, deserialized.getEndSequenceNumbers().get("1"));
  }

  @Test
  public void testDeserializationWithStringValues() throws Exception
  {
    String json = "{"
                  + "\"startSequenceNumbers\": {\"0\": \"100\", \"1\": \"200\"},"
                  + "\"endSequenceNumbers\": {\"0\": \"500\", \"1\": \"600\"}"
                  + "}";

    BoundedStreamConfig config = mapper.readValue(json, BoundedStreamConfig.class);

    Assert.assertEquals(2, config.getStartSequenceNumbers().size());
    Assert.assertEquals(2, config.getEndSequenceNumbers().size());
  }

  @Test
  public void testDeserializationWithMixedTypes() throws Exception
  {
    String json = "{"
                  + "\"startSequenceNumbers\": {\"0\": 100, \"1\": \"200\"},"
                  + "\"endSequenceNumbers\": {\"0\": 500, \"1\": \"600\"}"
                  + "}";

    BoundedStreamConfig config = mapper.readValue(json, BoundedStreamConfig.class);

    Assert.assertEquals(2, config.getStartSequenceNumbers().size());
    Assert.assertEquals(2, config.getEndSequenceNumbers().size());
  }
}
