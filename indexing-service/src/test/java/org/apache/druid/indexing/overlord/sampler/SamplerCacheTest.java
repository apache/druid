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

package org.apache.druid.indexing.overlord.sampler;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SamplerCacheTest
{
  private static final String KEY_1 = "abcdefghijklmnopqrstuvwxyz";
  private static final String KEY_2 = "1234567890!@#$%^&*()";

  private static final byte[] VALUE_1_1 = StringUtils.toUtf8("The quick");
  private static final byte[] VALUE_1_2 = StringUtils.toUtf8("brown fox");
  private static final byte[] VALUE_1_3 = StringUtils.toUtf8("jumps over");
  private static final byte[] VALUE_2_1 = StringUtils.toUtf8("the lazy");
  private static final byte[] VALUE_2_2 = StringUtils.toUtf8("Druid");

  private static final StringInputRowParser PARSER = new StringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec(null, null, DateTimes.of("1970")),
          new DimensionsSpec(null),
          null,
          ImmutableList.of("col"),
          false,
          0
      ),
      StandardCharsets.UTF_8.name()
  );

  private SamplerCache cache;
  private File tempDir;

  @Before
  public void setupTest()
  {
    cache = new SamplerCache(MapCache.create(100000));
    tempDir = Files.createTempDir();
  }

  @After
  public void teardownTest() throws IOException
  {
    FileUtils.deleteDirectory(tempDir);
  }

  @Test
  public void testOneEntryNextRowWithRaw() throws IOException
  {
    cache.put(KEY_1, ImmutableList.of(VALUE_1_1, VALUE_1_2, VALUE_1_3));

    for (int i = 0; i < 4; i++) {
      Firehose firehose1 = cache.getAsFirehoseFactory(KEY_1, PARSER).connectForSampler(PARSER, tempDir);

      Assert.assertTrue(firehose1.hasMore());

      InputRowPlusRaw row = firehose1.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_1_1, row.getRaw());
      Assert.assertEquals("The quick", row.getInputRow().getDimension("col").get(0));
      row = firehose1.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_1_2, row.getRaw());
      Assert.assertEquals("brown fox", row.getInputRow().getDimension("col").get(0));
      row = firehose1.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_1_3, row.getRaw());
      Assert.assertEquals("jumps over", row.getInputRow().getDimension("col").get(0));

      Assert.assertFalse(firehose1.hasMore());

      firehose1.close();

      if (i % 2 == 1) {
        FirehoseFactory firehoseFactory2 = cache.getAsFirehoseFactory(KEY_2, PARSER);
        Assert.assertNull(firehoseFactory2);
      }
    }
  }

  @Test
  public void testOneEntryNextRow() throws IOException
  {
    cache.put(KEY_1, ImmutableList.of(VALUE_1_1, VALUE_1_2, VALUE_1_3));

    Firehose firehose = cache.getAsFirehoseFactory(KEY_1, PARSER).connectForSampler(PARSER, tempDir);

    Assert.assertTrue(firehose.hasMore());

    InputRow row = firehose.nextRow();
    Assert.assertEquals("The quick", row.getDimension("col").get(0));
    row = firehose.nextRow();
    Assert.assertEquals("brown fox", row.getDimension("col").get(0));
    row = firehose.nextRow();
    Assert.assertEquals("jumps over", row.getDimension("col").get(0));

    Assert.assertFalse(firehose.hasMore());

    firehose.close();
  }

  @Test
  public void testTwoEntriesNextRowWithRaw() throws IOException
  {
    cache.put(KEY_1, ImmutableList.of(VALUE_1_1, VALUE_1_2, VALUE_1_3));
    cache.put(KEY_2, ImmutableList.of(VALUE_2_1, VALUE_2_2));

    for (int i = 0; i < 4; i++) {
      Firehose firehose1 = cache.getAsFirehoseFactory(KEY_1, PARSER).connectForSampler(PARSER, tempDir);

      Assert.assertTrue(firehose1.hasMore());

      InputRowPlusRaw row = firehose1.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_1_1, row.getRaw());
      Assert.assertEquals("The quick", row.getInputRow().getDimension("col").get(0));
      row = firehose1.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_1_2, row.getRaw());
      Assert.assertEquals("brown fox", row.getInputRow().getDimension("col").get(0));
      row = firehose1.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_1_3, row.getRaw());
      Assert.assertEquals("jumps over", row.getInputRow().getDimension("col").get(0));

      Assert.assertFalse(firehose1.hasMore());

      firehose1.close();

      Firehose firehose2 = cache.getAsFirehoseFactory(KEY_2, PARSER).connectForSampler(PARSER, tempDir);

      Assert.assertTrue(firehose2.hasMore());

      row = firehose2.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_2_1, row.getRaw());
      Assert.assertEquals("the lazy", row.getInputRow().getDimension("col").get(0));
      row = firehose2.nextRowWithRaw();
      Assert.assertArrayEquals(VALUE_2_2, row.getRaw());
      Assert.assertEquals("Druid", row.getInputRow().getDimension("col").get(0));

      Assert.assertFalse(firehose2.hasMore());

      firehose2.close();
    }
  }
}
