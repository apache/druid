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
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class DruidJsonValidatorTest
{
  private File inputFile;
  private final Injector injector = GuiceInjectors.makeStartupInjector();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
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
    Assert.assertTrue(command instanceof Runnable);

    injector.injectMembers(command);
    return (Runnable) command;
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExceptionCase()
  {
    parseCommand("validator", "-f", inputFile.getAbsolutePath(), "-t", "").run();
  }

  @Test(expected = RuntimeException.class)
  public void testExceptionCaseNoFile()
  {
    parseCommand("validator", "-f", "", "-t", "query").run();
  }

  @Test(expected = RuntimeException.class)
  public void testParseValidatorInvalid()
  {
    parseCommand(
        "validator",
        "-f", "simple_test_data_record_parser_invalid.json",
        "-t", "parse"
    ).run();
  }

  @Test
  public void testParseValidator()
  {
    Runnable command = parseCommand(
        "validator",
        "-f", "simple_test_data_record_parser.json",
        "-r", "simple_test_data.tsv",
        "-t", "parse"
    );
    command.run();

    Writer writer = new StringWriter()
    {
      @Override
      public void write(String str)
      {
        super.write(str + '\n');
      }
    };
    DruidJsonValidator druidJsonValidator = (DruidJsonValidator) command;
    druidJsonValidator.setLogWriter(writer);
    druidJsonValidator.run();

    String expected = "loading parse spec from resource 'simple_test_data_record_parser.json'\n" +
                      "loading data from resource 'simple_test_data.tsv'\n" +
                      "2014-10-20T00:00:00.000Z\tproduct_1\n";

    Assert.assertEquals(expected, writer.toString());
  }

  @Test
  public void testTaskValidator() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final IndexTask task = new IndexTask(
        null,
        new TaskResource("rofl", 2),
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[0],
                new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
                null,
                jsonMapper
            ),
            new IndexTask.IndexIOConfig(
                null,
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
                IndexSpec.DEFAULT,
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

  @After
  public void tearDown()
  {
    temporaryFolder.delete();
  }
}
