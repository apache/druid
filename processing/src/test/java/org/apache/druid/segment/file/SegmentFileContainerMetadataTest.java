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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SegmentFileContainerMetadataTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(SegmentFileContainerMetadata.class).usingGetClass().verify();
  }

  @Test
  void testSerdeWithNamedBundle() throws Exception
  {
    final SegmentFileContainerMetadata metadata = new SegmentFileContainerMetadata(100, 4096, "projA");
    final String json = JSON_MAPPER.writeValueAsString(metadata);
    Assertions.assertTrue(json.contains("\"bundle\":\"projA\""), "bundle must be present in serialized JSON: " + json);
    Assertions.assertEquals(metadata, JSON_MAPPER.readValue(json, SegmentFileContainerMetadata.class));
  }

  @Test
  void testNullBundleNormalizesToRootAndOmitsFromJson() throws Exception
  {
    // Null in the constructor is the writer-side equivalent of "no explicit startFileBundle call"; the field
    // normalizes to ROOT_BUNDLE_NAME, and the default value is omitted from JSON so segments without explicit
    // bundles stay compact on disk.
    final SegmentFileContainerMetadata metadata = new SegmentFileContainerMetadata(0, 1024, null);
    Assertions.assertEquals(SegmentFileBuilder.ROOT_BUNDLE_NAME, metadata.getBundle());
    final String json = JSON_MAPPER.writeValueAsString(metadata);
    Assertions.assertFalse(json.contains("bundle"), "default bundle must be omitted from JSON, got: " + json);
    Assertions.assertEquals(metadata, JSON_MAPPER.readValue(json, SegmentFileContainerMetadata.class));
  }

  @Test
  void testExplicitRootBundleAlsoOmitsFromJson() throws Exception
  {
    // Passing ROOT_BUNDLE_NAME explicitly is equivalent to passing null; both normalize to the default and both
    // omit the field from JSON.
    final SegmentFileContainerMetadata metadata =
        new SegmentFileContainerMetadata(0, 1024, SegmentFileBuilder.ROOT_BUNDLE_NAME);
    final String json = JSON_MAPPER.writeValueAsString(metadata);
    Assertions.assertFalse(json.contains("bundle"), "explicit root bundle must be omitted from JSON, got: " + json);
    Assertions.assertEquals(metadata, JSON_MAPPER.readValue(json, SegmentFileContainerMetadata.class));
  }

  @Test
  void testDeserializeJsonWithoutBundleFieldDefaultsToRoot() throws Exception
  {
    // Bytes produced by a writer that didn't include a bundle field (old segments, or new segments without
    // explicit startFileBundle) must deserialize to the ROOT_BUNDLE_NAME default.
    final String json = "{\"startOffset\":42,\"size\":8192}";
    final SegmentFileContainerMetadata metadata = JSON_MAPPER.readValue(json, SegmentFileContainerMetadata.class);
    Assertions.assertEquals(42, metadata.getStartOffset());
    Assertions.assertEquals(8192, metadata.getSize());
    Assertions.assertEquals(SegmentFileBuilder.ROOT_BUNDLE_NAME, metadata.getBundle());
  }
}
