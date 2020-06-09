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

package org.apache.druid.inputsource.hdfs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.storage.hdfs.HdfsStorageDruidModule;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Enclosed.class)
public class HdfsInputSourceTest extends InitializedNullHandlingTest
{
  private static final String PATH = "/foo/bar";
  private static final Configuration CONFIGURATION = new Configuration();
  private static final String COLUMN = "value";
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec(null, null, null),
      DimensionsSpec.EMPTY,
      Collections.emptyList()
  );
  private static final InputFormat INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList(TimestampSpec.DEFAULT_COLUMN, COLUMN),
      null,
      false,
      null,
      0
  );

  public static class SerializeDeserializeTest
  {
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private HdfsInputSource.Builder hdfsInputSourceBuilder;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup()
    {
      hdfsInputSourceBuilder = HdfsInputSource.builder()
                                              .paths(PATH)
                                              .configuration(CONFIGURATION);
    }

    @Test
    public void requiresPathsAsStringOrArrayOfStrings()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("'paths' must be a string or an array of strings");

      hdfsInputSourceBuilder.paths(Arrays.asList("a", 1)).build();
    }

    @Test
    public void serializesDeserializesWithArrayPaths()
    {
      Wrapper target = new Wrapper(hdfsInputSourceBuilder.paths(Collections.singletonList(PATH)));
      testSerializesDeserializes(target);
    }

    @Test
    public void serializesDeserializesStringPaths()
    {
      Wrapper target = new Wrapper(hdfsInputSourceBuilder.paths(PATH));
      testSerializesDeserializes(target);
    }

    private static void testSerializesDeserializes(Wrapper hdfsInputSourceWrapper)
    {
      try {
        String serialized = OBJECT_MAPPER.writeValueAsString(hdfsInputSourceWrapper);
        Wrapper deserialized = OBJECT_MAPPER.readValue(serialized, Wrapper.class);
        Assert.assertEquals(serialized, OBJECT_MAPPER.writeValueAsString(deserialized));
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private static ObjectMapper createObjectMapper()
    {
      final ObjectMapper mapper = new ObjectMapper();
      mapper.setInjectableValues(new InjectableValues.Std().addValue(Configuration.class, new Configuration()));
      new HdfsStorageDruidModule().getJacksonModules().forEach(mapper::registerModule);
      return mapper;
    }

    // Helper to test HdfsInputSource is added correctly to HdfsStorageDruidModule
    private static class Wrapper
    {
      @JsonProperty
      InputSource inputSource;

      @SuppressWarnings("unused")  // used by Jackson
      private Wrapper()
      {
      }

      Wrapper(HdfsInputSource.Builder hdfsInputSourceBuilder)
      {
        this.inputSource = hdfsInputSourceBuilder.build();
      }
    }
  }

  public static class ReaderTest
  {
    private static final String PATH = "/test";
    private static final int NUM_FILE = 3;
    private static final String KEY_VALUE_SEPARATOR = ",";
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private MiniDFSCluster dfsCluster;
    private HdfsInputSource target;
    private Set<Path> paths;
    private Map<Long, String> timestampToValue;

    @Before
    public void setup() throws IOException
    {
      timestampToValue = new HashMap<>();

      File dir = temporaryFolder.getRoot();
      Configuration configuration = new Configuration(true);
      configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dir.getAbsolutePath());
      dfsCluster = new MiniDFSCluster.Builder(configuration).build();

      paths = IntStream.range(0, NUM_FILE)
                       .mapToObj(
                           i -> {
                             char value = ALPHABET.charAt(i % ALPHABET.length());
                             timestampToValue.put((long) i, Character.toString(value));
                             return createFile(
                                 dfsCluster,
                                 String.valueOf(i),
                                 i + KEY_VALUE_SEPARATOR + value
                             );
                           }
                       )
                       .collect(Collectors.toSet());

      target = HdfsInputSource.builder()
                              .paths(dfsCluster.getURI() + PATH + "*")
                              .configuration(CONFIGURATION)
                              .build();
    }

    @After
    public void teardown()
    {
      if (dfsCluster != null) {
        dfsCluster.shutdown(true);
      }
    }

    private static Path createFile(MiniDFSCluster dfsCluster, String pathSuffix, String contents)
    {
      try {
        Path path = new Path(PATH + pathSuffix);
        try (Writer writer = new BufferedWriter(
            new OutputStreamWriter(dfsCluster.getFileSystem().create(path), StandardCharsets.UTF_8)
        )) {
          writer.write(contents);
        }
        return path;
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Test
    public void readsSplitsCorrectly() throws IOException
    {
      InputSourceReader reader = target.formattableReader(INPUT_ROW_SCHEMA, INPUT_FORMAT, null);

      Map<Long, String> actualTimestampToValue = new HashMap<>();
      try (CloseableIterator<InputRow> iterator = reader.read()) {
        while (iterator.hasNext()) {
          InputRow row = iterator.next();
          actualTimestampToValue.put(row.getTimestampFromEpoch(), row.getDimension(COLUMN).get(0));
        }
      }

      Assert.assertEquals(timestampToValue, actualTimestampToValue);
    }

    @Test
    public void hasCorrectSplits() throws IOException
    {
      // Set maxSplitSize to 1 so that each inputSplit has only one object
      List<InputSplit<List<Path>>> splits = target.createSplits(null, new MaxSizeSplitHintSpec(1L))
                                                  .collect(Collectors.toList());
      splits.forEach(split -> Assert.assertEquals(1, split.get().size()));
      Set<Path> actualPaths = splits.stream()
                                    .flatMap(split -> split.get().stream())
                                    .map(Path::getPathWithoutSchemeAndAuthority)
                                    .collect(Collectors.toSet());
      Assert.assertEquals(paths, actualPaths);
    }

    @Test
    public void createSplitsRespectSplitHintSpec() throws IOException
    {
      List<InputSplit<List<Path>>> splits = target.createSplits(null, new MaxSizeSplitHintSpec(7L))
                                                  .collect(Collectors.toList());
      Assert.assertEquals(2, splits.size());
      Assert.assertEquals(2, splits.get(0).get().size());
      Assert.assertEquals(1, splits.get(1).get().size());
    }

    @Test
    public void hasCorrectNumberOfSplits() throws IOException
    {
      // Set maxSplitSize to 1 so that each inputSplit has only one object
      int numSplits = target.estimateNumSplits(null, new MaxSizeSplitHintSpec(1L));
      Assert.assertEquals(NUM_FILE, numSplits);
    }

    @Test
    public void createCorrectInputSourceWithSplit() throws Exception
    {
      // Set maxSplitSize to 1 so that each inputSplit has only one object
      List<InputSplit<List<Path>>> splits = target.createSplits(null, new MaxSizeSplitHintSpec(1L))
                                                  .collect(Collectors.toList());

      for (InputSplit<List<Path>> split : splits) {
        String expectedPath = Iterables.getOnlyElement(split.get()).toString();
        HdfsInputSource inputSource = (HdfsInputSource) target.withSplit(split);
        String actualPath = Iterables.getOnlyElement(inputSource.getInputPaths());
        Assert.assertEquals(expectedPath, actualPath);
      }
    }
  }

  public static class EmptyPathsTest
  {
    private HdfsInputSource target;

    @Before
    public void setup()
    {
      target = HdfsInputSource.builder()
                              .paths(Collections.emptyList())
                              .configuration(CONFIGURATION)
                              .build();
    }

    @Test
    public void readsSplitsCorrectly() throws IOException
    {
      InputSourceReader reader = target.formattableReader(INPUT_ROW_SCHEMA, INPUT_FORMAT, null);

      try (CloseableIterator<InputRow> iterator = reader.read()) {
        Assert.assertFalse(iterator.hasNext());
      }
    }

    @Test
    public void hasCorrectSplits() throws IOException
    {
      List<InputSplit<List<Path>>> splits = target.createSplits(null, null)
                                                  .collect(Collectors.toList());
      Assert.assertTrue(String.valueOf(splits), splits.isEmpty());
    }

    @Test
    public void hasCorrectNumberOfSplits() throws IOException
    {
      int numSplits = target.estimateNumSplits(null, null);
      Assert.assertEquals(0, numSplits);
    }
  }
}
