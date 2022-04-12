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

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertThrows;


public class DruidSegmentInputFormatTest
{

  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "name"))),
      ColumnsFilter.all()
  );


  @Test
  public void testDruidSegmentInputEntityReader()
  {
    DruidSegmentInputFormat format = new DruidSegmentInputFormat(null, null);
    InputEntityReader reader = format.createReader(
        INPUT_ROW_SCHEMA,
        DruidSegmentReaderTest.makeInputEntity(Intervals.of("2000/P1D"), null),
        null
    );
    Assert.assertTrue(reader instanceof DruidSegmentReader);
  }


  @Test
  public void testDruidTombstoneSegmentInputEntityReader()
  {
    DruidSegmentInputFormat format = new DruidSegmentInputFormat(null, null);
    InputEntityReader reader = format.createReader(
        INPUT_ROW_SCHEMA,
        DruidSegmentReaderTest.makeTombstoneInputEntity(Intervals.of("2000/P1D")),
        null
    );
    Assert.assertTrue(reader instanceof DruidTombstoneSegmentReader);
  }

  @Test
  public void testDruidSegmentInputEntityReaderBadEntity()
  {
    DruidSegmentInputFormat format = new DruidSegmentInputFormat(null, null);
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      format.createReader(
          INPUT_ROW_SCHEMA,
          new FileEntity(null),
          null
      );
    });
    String expectedMessage =
        "org.apache.druid.indexing.input.DruidSegmentInputEntity required, but org.apache.druid.data.input.impl.FileEntity provided.";
    String actualMessage = exception.getMessage();
    Assert.assertEquals(expectedMessage, actualMessage);
  }
}
