/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli.validate;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import io.airlift.airline.Cli;
import io.druid.guice.FirehoseModule;
import io.druid.guice.GuiceInjectors;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Period;
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
    for (final Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }

    final RealtimeIndexTask task = new RealtimeIndexTask(
        null,
        new TaskResource("rofl", 2),
        new FireDepartment(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[0],
                new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
                null,
                jsonMapper
            ),
            new RealtimeIOConfig(
                new LocalFirehoseFactory(new File("lol"), "rofl", null), new PlumberSchool()
                {
                  @Override
                  public Plumber findPlumber(
                      DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
                  )
                  {
                    return null;
                  }
                },
                null
            ),

            new RealtimeTuningConfig(
                1,
                null,
                new Period("PT10M"),
                null,
                null,
                null,
                null,
                1,
                NoneShardSpec.instance(),
                new IndexSpec(),
                null,
                0,
                0,
                true,
                null,
                null,
                null,
                null
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
