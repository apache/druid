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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DataSegmentAndIndexZipFilePathTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.dummy("data-source", 1);
  private static final SegmentId OTHER_SEGMENT_ID = SegmentId.dummy("data-source2", 1);
  private static final DataSegment SEGMENT = new DataSegment(
      SEGMENT_ID,
      null,
      null,
      null,
      new NumberedShardSpec(1, 10),
      null,
      0,
      0
  );
  private static final DataSegment OTHER_SEGMENT = new DataSegment(
      OTHER_SEGMENT_ID,
      null,
      null,
      null,
      new NumberedShardSpec(1, 10),
      null,
      0,
      0
  );

  private DataSegmentAndIndexZipFilePath target;

  @Test
  public void test_equals_otherNull_notEqual()
  {
    String tmpPath = "tmpPath";
    String finalPath = "finalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );
    Assert.assertNotEquals(target, null);
  }

  @Test
  public void test_equals_differentSegmentId_notEqual()
  {
    String tmpPath = "tmpPath";
    String finalPath = "finalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );

    DataSegmentAndIndexZipFilePath other = new DataSegmentAndIndexZipFilePath(
        OTHER_SEGMENT,
        tmpPath,
        finalPath
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void test_equals_differentTmpPath_notEqual()
  {
    String tmpPath = "tmpPath";
    String otherTmpPath = "otherTmpPath";
    String finalPath = "finalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );

    DataSegmentAndIndexZipFilePath other = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        otherTmpPath,
        finalPath
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void test_equals_differentFinalPath_notEqual()
  {
    String tmpPath = "tmpPath";
    String finalPath = "finalPath";
    String otherFinalPath = "otherFinalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );

    DataSegmentAndIndexZipFilePath other = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        otherFinalPath
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void test_equals_allFieldsEqualValue_equal()
  {
    String tmpPath = "tmpPath";
    String finalPath = "finalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );

    DataSegmentAndIndexZipFilePath other = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );
    Assert.assertEquals(target, other);
  }

  @Test
  public void test_equals_sameObject_equal()
  {
    String tmpPath = "tmpPath";
    String finalPath = "finalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );

    Assert.assertEquals(target, target);
  }

  @Test
  public void test_serde() throws IOException
  {
    String tmpPath = "tmpPath";
    String finalPath = "finalPath";
    target = new DataSegmentAndIndexZipFilePath(
        SEGMENT,
        tmpPath,
        finalPath
    );

    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(injectableValues);
    final String json = mapper.writeValueAsString(target);
    final DataSegmentAndIndexZipFilePath fromJson =
        mapper.readValue(json, DataSegmentAndIndexZipFilePath.class);
    Assert.assertEquals(target, fromJson);
  }
}
