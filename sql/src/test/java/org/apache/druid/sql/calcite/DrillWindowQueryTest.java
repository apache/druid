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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteStreams;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * These test cases are borrowed from the drill-test-framework at
 * https://github.com/apache/drill-test-framework
 * <p>
 * The Drill data sources are just accessing parquet files directly, we ingest
 * and index the data first via JSON (so that we avoid pulling in the parquet
 * dependencies here) and then run the queries over that.
 * <p>
 * The setup of the ingestion is done via code in this class, so any new data
 * source needs to be added through that manner. That said, these tests are
 * primarily being added to bootstrap our own test coverage of window functions,
 * so it is believed that most iteration on tests will happen through the
 * CalciteWindowQueryTest instead of this class.
 */
@RunWith(Parameterized.class)
public class DrillWindowQueryTest extends BaseCalciteQueryTest
{
  private static final Logger log = new Logger(DrillWindowQueryTest.class);

  static {
    NullHandling.initializeForTests();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object parametersForWindowQueryTest() throws Exception
  {
    final URL windowQueriesUrl = ClassLoader.getSystemResource("drill/window/queries/");
    File windowFolder = new File(windowQueriesUrl.toURI());
    int windowFolderPrefixSize = windowFolder.getAbsolutePath().length() + 1 /*
                                                                              * 1
                                                                              * for
                                                                              * the
                                                                              * ending
                                                                              * slash
                                                                              */;

    return FileUtils
        .streamFiles(windowFolder, true, "q")
        .map(file -> {
          final String filePath = file.getAbsolutePath();
          return filePath.substring(windowFolderPrefixSize, filePath.length() - 2);
        })
        .sorted()
        .toArray();
  }

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private final DrillTestCase testCase;

  public DrillWindowQueryTest(String filename) throws IOException
  {
    this.testCase = new DrillTestCase(filename);
  }

  static class DrillTestCase
  {
    private final String filename;
    private final String query;
    private final List<String[]> results;

    public DrillTestCase(String filename) throws IOException
    {
      this.filename = filename;
      this.query = readStringFromResource(".q");
      String resultsStr = readStringFromResource(".e");
      String[] lines = resultsStr.split("\n");
      results = new ArrayList<>();
      for (String string : lines) {
        String[] cols = string.split("\t");
        results.add(cols);
      }
    }

    @Nonnull
    private String getQueryString()
    {
      return query;
    }

    @Nonnull
    private List<String[]> getExpectedResults()
    {
      return results;
    }

    @SuppressWarnings({"UnstableApiUsage", "ConstantConditions"})
    @Nonnull
    private String readStringFromResource(String s) throws IOException
    {
      final String query;
      try (InputStream queryIn = ClassLoader.getSystemResourceAsStream("drill/window/queries/" + filename + s)) {
        query = new String(ByteStreams.toByteArray(queryIn), StandardCharsets.UTF_8);
      }
      return query;
    }
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      JoinableFactoryWrapper joinableFactory,
      Injector injector) throws IOException
  {
    final SpecificSegmentsQuerySegmentWalker retVal = super.createQuerySegmentWalker(
        conglomerate,
        joinableFactory,
        injector);

    attachIndex(
        retVal,
        "tblWnulls.parquet",
        new LongDimensionSchema("c1"),
        new StringDimensionSchema("c2"));

    // {"col0":1,"col1":65534,"col2":256.0,"col3":1234.9,"col4":73578580,"col5":1393720082338,"col6":421185052800000,"col7":false,"col8":"CA","col9":"AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXZ"}
    attachIndex(
        retVal,
        "allTypsUniq.parquet",
        new LongDimensionSchema("col0"),
        new LongDimensionSchema("col1"),
        new DoubleDimensionSchema("col2"),
        new DoubleDimensionSchema("col3"),
        new LongDimensionSchema("col4"),
        new LongDimensionSchema("col5"),
        new LongDimensionSchema("col6"),
        new StringDimensionSchema("col7"),
        new StringDimensionSchema("col8"),
        new StringDimensionSchema("col9"));

    return retVal;
  }

  public class TextualResultsVerifier implements ResultsVerifier
  {
    protected final List<String[]> expectedResults;
    @Nullable
    protected final RowSignature expectedResultRowSignature;
    private RowSignature currentRowSignature;

    public TextualResultsVerifier(List<String[]> expectedResults, RowSignature expectedSignature)
    {
      this.expectedResults = expectedResults;
      this.expectedResultRowSignature = expectedSignature;
    }

    @Override
    public void verifyRowSignature(RowSignature rowSignature)
    {
      if (expectedResultRowSignature != null) {
        Assert.assertEquals(expectedResultRowSignature, rowSignature);
      }
      currentRowSignature = rowSignature;
    }

    @Override
    public void verify(String sql, List<Object[]> results)
    {
      List<Object[]> parsedExpectedResults = parseResults(currentRowSignature, expectedResults);
      try {
        Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResults.size(), results.size());
        assertResultsEquals(sql, parsedExpectedResults, results);
      } catch (AssertionError e) {
        System.out.println("query: " + sql);
        displayResults(results);
        throw e;
      }
    }
  }

  private static List<Object[]> parseResults(RowSignature rs, List<String[]> results)
  {
    List<Object[]> ret = new ArrayList<>();
    for (Object[] row : results) {
      Object[] newRow = new Object[row.length];
      List<String> cc = rs.getColumnNames();
      for (int i = 0; i < cc.size(); i++) {
        ColumnType t = rs.getColumnType(i).get();
        assertNull(t.getComplexTypeName());

        Object val = row[i];

        if ("null".equals(val)) {
          val = null;
        } else {
          switch (t.getType())
          {
          case STRING:
            break;
          case LONG:
            val = Numbers.parseLong(val);
            break;
          case DOUBLE:
            val = Numbers.parseDoubleObject((String) val);
            break;
          default:
            throw new RuntimeException("unimplemented");
          }
        }
        newRow[i] = val;

      }
      ret.add(newRow);
    }
    return ret;
  }

  @Test
  public void windowQueryTest() throws Exception
  {
    Thread thread = null;
    String oldName = null;
    try {
      thread = Thread.currentThread();
      oldName = thread.getName();
      thread.setName("drillWindowQuery-" + testCase.filename);

      testBuilder()
          .skipVectorize(true)
          .queryContext(ImmutableMap.of(
              PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
              "windowsAllTheWayDown", true,
              QueryContexts.ENABLE_DEBUG, true,
              PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false))
          .sql(testCase.getQueryString())
          // .queryContext(ImmutableMap.of("windowsAreForClosers", true,
          // "windowsAllTheWayDown", true))
          .expectedResults(new TextualResultsVerifier(testCase.getExpectedResults(), null))
          .run();
    } finally {
      if (thread != null && oldName != null) {
        thread.setName(oldName);
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void attachIndex(SpecificSegmentsQuerySegmentWalker texasRanger, String dataSource, DimensionSchema... dims)
      throws IOException
  {
    ArrayList<String> dimensionNames = new ArrayList<>(dims.length);
    for (DimensionSchema dimension : dims) {
      dimensionNames.add(dimension.getName());
    }

    final File tmpFolder = temporaryFolder.newFolder();
    final QueryableIndex queryableIndex = IndexBuilder
        .create()
        .tmpDir(new File(tmpFolder, dataSource))
        .segmentWriteOutMediumFactory(OnHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(new IncrementalIndexSchema.Builder()
            .withRollup(false)
            .withDimensionsSpec(new DimensionsSpec(Arrays.asList(dims)))
            .build())
        .rows(
            () -> {
              try {
                return Iterators.transform(
                    MAPPER.readerFor(Map.class)
                        .readValues(
                            ClassLoader.getSystemResource("drill/window/datasources/" + dataSource + ".json")),
                    (Function<Map, InputRow>) input -> new MapBasedInputRow(0, dimensionNames, input));
              } catch (IOException e) {
                throw new RE(e, "problem reading file");
              }
            })
        .buildMMappedIndex();

    texasRanger.add(
        DataSegment.builder()
            .dataSource(dataSource)
            .interval(Intervals.ETERNITY)
            .version("1")
            .shardSpec(new NumberedShardSpec(0, 0))
            .size(0)
            .build(),
        queryableIndex);
  }
}
