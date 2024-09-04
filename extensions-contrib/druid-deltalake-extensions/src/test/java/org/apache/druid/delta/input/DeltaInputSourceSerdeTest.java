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

package org.apache.druid.delta.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.apache.druid.delta.common.DeltaLakeDruidModule;
import org.apache.druid.delta.filter.DeltaAndFilter;
import org.apache.druid.delta.filter.DeltaLessThanFilter;
import org.apache.druid.delta.snapshot.LatestSnapshot;
import org.apache.druid.delta.snapshot.VersionedSnapshot;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class DeltaInputSourceSerdeTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper()
      .registerModules(new DeltaLakeDruidModule().getJacksonModules());

  @Test
  public void testDeltaInputSourceDeserializationWithNoFilter() throws JsonProcessingException
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\"\n"
                           + "    }";

    final DeltaInputSource deltaInputSource = OBJECT_MAPPER.readValue(payload, DeltaInputSource.class);
    Assert.assertEquals("foo/bar", deltaInputSource.getTablePath());
    Assert.assertNull(deltaInputSource.getFilter());
    Assert.assertTrue(deltaInputSource.getSnapshot() instanceof LatestSnapshot);
  }

  @Test
  public void testDeltaInputSourceDeserializationWithLessThanFilter() throws JsonProcessingException
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\",\n"
                           + "      \"filter\": {\n"
                           + "        \"type\": \"<\",\n"
                           + "        \"column\": \"age\",\n"
                           + "        \"value\": \"20\"\n"
                           + "      }\n"
                           + "    }";

    final DeltaInputSource deltaInputSource = OBJECT_MAPPER.readValue(payload, DeltaInputSource.class);
    Assert.assertEquals("foo/bar", deltaInputSource.getTablePath());
    Assert.assertTrue(deltaInputSource.getFilter() instanceof DeltaLessThanFilter);
    Assert.assertTrue(deltaInputSource.getSnapshot() instanceof LatestSnapshot);
  }

  @Test
  public void testDeltaInputSourceDeserializationWithAndFilter() throws JsonProcessingException
  {
    final String payload = "{\n"
                     + "      \"type\": \"delta\",\n"
                     + "      \"tablePath\": \"s3://foo/bar/baz\",\n"
                     + "      \"filter\": {\n"
                     + "        \"type\": \"and\",\n"
                     + "        \"filters\": [\n"
                     + "          {\n"
                     + "            \"type\": \"<=\",\n"
                     + "            \"column\": \"age\",\n"
                     + "            \"value\": \"30\"\n"
                     + "          },\n"
                     + "          {\n"
                     + "            \"type\": \">=\",\n"
                     + "            \"column\": \"name\",\n"
                     + "            \"value\": \"Employee4\"\n"
                     + "          }\n"
                     + "        ]\n"
                     + "      }\n"
                     + "    }";

    final DeltaInputSource deltaInputSource = OBJECT_MAPPER.readValue(payload, DeltaInputSource.class);
    Assert.assertEquals("s3://foo/bar/baz", deltaInputSource.getTablePath());
    Assert.assertTrue(deltaInputSource.getFilter() instanceof DeltaAndFilter);
    Assert.assertTrue(deltaInputSource.getSnapshot() instanceof LatestSnapshot);
  }

  @Test
  public void testDeltaInputSourceDeserializationWithNoTablePath()
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"filter\": {\n"
                           + "        \"type\": \"<\",\n"
                           + "        \"column\": \"age\",\n"
                           + "        \"value\": \"20\"\n"
                           + "      }\n"
                           + "    }";

    final ValueInstantiationException exception = Assert.assertThrows(
        ValueInstantiationException.class,
        () -> OBJECT_MAPPER.readValue(payload, DeltaInputSource.class)
    );

    Assert.assertTrue(
        exception.getCause().getMessage().contains(
            "tablePath cannot be null."
        )
    );
  }

  @Test
  public void testDeltaInputSourceDeserializationWithNoFilterColumn()
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\",\n"
                           + "      \"filter\": {\n"
                           + "        \"type\": \">=\",\n"
                           + "        \"value\": \"20\"\n"
                           + "      }\n"
                           + "    }";

    final ValueInstantiationException exception = Assert.assertThrows(
        ValueInstantiationException.class,
        () -> OBJECT_MAPPER.readValue(payload, DeltaInputSource.class)
    );

    Assert.assertEquals(
        "column is a required field for >= filter.",
        exception.getCause().getMessage()
    );
  }

  @Test
  public void testDeltaInputSourceLatestSnapshot() throws JsonProcessingException
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\",\n"
                           + "      \"snapshot\": {\n"
                           + "        \"type\": \"latest\"\n"
                           + "      }\n"
                           + "    }";

    final DeltaInputSource deltaInputSource = OBJECT_MAPPER.readValue(payload, DeltaInputSource.class);
    Assert.assertEquals("foo/bar", deltaInputSource.getTablePath());
    Assert.assertTrue(deltaInputSource.getSnapshot() instanceof LatestSnapshot);
  }

  @Test
  public void testDeltaInputSourceVersionedSnapshot() throws JsonProcessingException
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\",\n"
                           + "      \"snapshot\": {\n"
                           + "        \"type\": \"version\",\n"
                           + "        \"version\": 56\n"
                           + "      }\n"
                           + "    }";

    final DeltaInputSource deltaInputSource = OBJECT_MAPPER.readValue(payload, DeltaInputSource.class);
    Assert.assertEquals("foo/bar", deltaInputSource.getTablePath());
    Assert.assertTrue(deltaInputSource.getSnapshot() instanceof VersionedSnapshot);
  }

  @Test
  public void testDeltaInputSourceVersionSnapshotMissingVersion()
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\",\n"
                           + "      \"snapshot\": {\n"
                           + "        \"type\": \"version\"\n"
                           + "      }\n"
                           + "    }";

    final ValueInstantiationException exception = Assert.assertThrows(
        ValueInstantiationException.class,
        () -> OBJECT_MAPPER.readValue(payload, DeltaInputSource.class)
    );

    Assert.assertEquals(
        "version cannot be empty or null for versioned snapshot reads.",
        exception.getCause().getMessage()
    );
  }

  @Test
  public void testDeltaInputSourceInvalidSnapshot()
  {
    final String payload = "{\n"
                           + "      \"type\": \"delta\",\n"
                           + "      \"tablePath\": \"foo/bar\",\n"
                           + "      \"snapshot\": {\n"
                           + "        \"type\": \"jdldld\"\n"
                           + "      }\n"
                           + "    }";

    Assert.assertThrows(
        InvalidTypeIdException.class,
        () -> OBJECT_MAPPER.readValue(payload, DeltaInputSource.class)
    );
  }
}
