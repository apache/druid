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

package org.apache.druid.cli;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.ServerInjectorBuilder;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ValidateSegmentsTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testValidateSegments() throws IOException
  {

    JsonInputFormat inputFormat = new JsonInputFormat(
        JSONPathSpec.DEFAULT,
        null,
        null,
        null,
        null
    );
    IndexBuilder bob = IndexBuilder.create()
                                   .tmpDir(temporaryFolder.newFolder())
                                   .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                   .schema(
                                       new IncrementalIndexSchema.Builder()
                                           .withTimestampSpec(NestedDataTestUtils.AUTO_SCHEMA.getTimestampSpec())
                                           .withDimensionsSpec(NestedDataTestUtils.AUTO_SCHEMA.getDimensionsSpec())
                                           .withMetrics(
                                               new CountAggregatorFactory("cnt")
                                           )
                                           .withRollup(false)
                                           .build()
                                   )
                                   .inputSource(
                                       ResourceInputSource.of(
                                           NestedDataTestUtils.class.getClassLoader(),
                                           NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE
                                       )
                                   )
                                   .inputFormat(inputFormat)
                                   .inputTmpDir(temporaryFolder.newFolder());

    final File segment1 = bob.buildMMappedIndexFile();
    final File segment2 = bob.buildMMappedIndexFile();
    final Injector injector = Mockito.mock(Injector.class);
    Mockito.when(injector.getInstance(IndexIO.class)).thenReturn(bob.getIndexIO());
    ValidateSegments validator = new ValidateSegments() {
      @Override
      public Injector makeInjector()
      {
        return injector;
      }
    };
    validator.directories = Arrays.asList(segment1.getAbsolutePath(), segment2.getAbsolutePath());
    // if this doesn't pass, it throws a runtime exception, which would fail the test
    validator.run();
  }

  @Test
  public void testGetModules()
  {
    ValidateSegments validator = new ValidateSegments();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        new StartupInjectorBuilder().forServer().build(),
        Collections.emptySet(),
        validator.getModules()
    );
    Assert.assertNotNull(injector.getInstance(ColumnConfig.class));
    Assert.assertEquals("druid/tool", injector.getInstance(Key.get(String.class, Names.named("serviceName"))));
    Assert.assertEquals(9999, (int) injector.getInstance(Key.get(Integer.class, Names.named("servicePort"))));
    Assert.assertEquals(-1, (int) injector.getInstance(Key.get(Integer.class, Names.named("tlsServicePort"))));
  }
}
