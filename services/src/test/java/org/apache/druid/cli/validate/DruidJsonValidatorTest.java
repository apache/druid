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

package org.apache.druid.cli.validate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.Cli;
import com.google.inject.Injector;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.junit5.ExpectedToThrow;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.apache.druid.testing.junit5.TempDirExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;

public class DruidJsonValidatorTest
{
  private File inputFile;
  private final Injector injector = GuiceInjectors.makeStartupInjector();

  @RegisterExtension
  public TempDirExtension temporaryFolder = new TempDirExtension();

  @BeforeEach
  public void setUp() throws IOException
  {
    inputFile = temporaryFolder.newFile();
  }

  private Runnable parseCommand(String... args)
  {
    Cli<?> parser = Cli.builder("validator")
                       .withCommand(DruidJsonValidator.class)
                       .build();

    Object command = parser.parse(args);
    JUnit5Assertions.assertTrue(command instanceof Runnable);

    injector.injectMembers(command);
    return (Runnable) command;
  }

  @Test
  @ExpectedToThrow(UnsupportedOperationException.class)
  public void testExceptionCase()
  {
    parseCommand("validator", "-f", inputFile.getAbsolutePath(), "-t", "").run();
  }

  @Test
  @ExpectedToThrow(RuntimeException.class)
  public void testExceptionCaseNoFile()
  {
    parseCommand("validator", "-f", "", "-t", "query").run();
  }

  @Test
  public void testTaskValidator() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final IndexTask task = new IndexTask(
        null,
        new TaskResource("rofl", 2),
        new IndexTask.IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(TimestampSpec.DEFAULT)
                      .withGranularity(new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null))
                      .build(),
            new IndexTask.IndexIOConfig(
                new LocalInputSource(new File("lol"), "rofl"),
                new JsonInputFormat(null, null, null, null, null),
                false,
                false
            ),

            new IndexTask.IndexTuningConfig(
                null,
                null,
                null,
                10,
                null,
                null,
                null,
                null,
                null,
                null,
                new DynamicPartitionsSpec(10000, null),
                IndexSpec.getDefault(),
                null,
                3,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                2
            )
        ),
        null
    );

    File tmp = temporaryFolder.newFile("test_task.json");
    jsonMapper.writeValue(tmp, task);

    parseCommand("validator", "-f", tmp.getAbsolutePath(), "-t", "task").run();
  }

  @AfterEach
  public void tearDown()
  {
  }
}
