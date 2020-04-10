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

package org.apache.druid.data.input.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.AvroHadoopInputRowParserTest;
import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class AvroOCFReaderTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testParse() throws IOException
  {
    final GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final File someAvroFile = AvroHadoopInputRowParserTest.createAvroFile(someAvroDatum);

    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto", null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
        "eventType")));
    final AvroOCFInputFormat inputFormat = new AvroOCFInputFormat(null, null);
    final InputRowSchema schema = new InputRowSchema(timestampSpec, dimensionsSpec, ImmutableList.of("someLong"));
    final FileEntity entity = new FileEntity(someAvroFile);
    final InputEntityReader reader = inputFormat.createReader(schema, entity, temporaryFolder.newFolder());

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      Assert.assertEquals(DateTimes.of("2015-10-25T19:30:00.000Z"), row.getTimestamp());
      Assert.assertEquals("type-a", Iterables.getOnlyElement(row.getDimension("eventType")));
      Assert.assertEquals(679865987569912369L, row.getMetric("someLong"));
      Assert.assertFalse(iterator.hasNext());
    }
  }
}
