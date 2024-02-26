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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;
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
import org.apache.druid.java.util.common.parsers.TimestampParser;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryResults;
import org.apache.druid.sql.calcite.planner.PlannerCaptureHook;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

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
public class DrillWindowQueryTest extends BaseCalciteQueryTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private DrillTestCase testCase = null;

  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public TestRule disableWhenNonSqlCompat = DisableUnless.SQL_COMPATIBLE;

  @Rule
  public NotYetSupportedProcessor ignoreProcessor = new NotYetSupportedProcessor();

  @Rule
  public DrillTestCaseLoaderRule drillTestCaseRule = new DrillTestCaseLoaderRule();

  @Test
  void ensureAllDeclared() throws Exception
  {
    final URL windowQueriesUrl = ClassLoader.getSystemResource("drill/window/queries/");
    Path windowFolder = new File(windowQueriesUrl.toURI()).toPath();

    Set<String> allCases = FileUtils
        .streamFiles(windowFolder.toFile(), true, "q")
        .map(file -> {
          return windowFolder.relativize(file.toPath()).toString();
        })
        .sorted().collect(Collectors.toSet());

    for (Method method : DrillWindowQueryTest.class.getDeclaredMethods()) {
      DrillTest ann = method.getAnnotation(DrillTest.class);
      if (method.getAnnotation(Test.class) == null || ann == null) {
        continue;
      }
      if (allCases.remove(ann.value() + ".q")) {
        continue;
      }
      fail(String.format(Locale.ENGLISH, "Testcase [%s] references invalid file [%s].", method.getName(), ann.value()));
    }

    for (String string : allCases) {
      string = string.substring(0, string.lastIndexOf('.'));
      System.out.printf(Locale.ENGLISH, "@%s( \"%s\" )\n"
          + "@Test\n"
          + "public void test_%s() {\n"
          + "    windowQueryTest();\n"
          + "}\n",
          DrillTest.class.getSimpleName(),
          string,
          string.replace('/', '_'));
    }
    assertEquals(0, allCases.size(), "Found some non-declared testcases; please add the new testcases printed to the console!");
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  public @interface DrillTest
  {
    /**
     * Name of the file this test should execute.
     */
    String value();

    private static File newFolder(File root, String... subDirs) throws IOException {
      String subFolder = String.join("/", subDirs);
      File result = new File(root, subFolder);
      if (!result.mkdirs()) {
        throw new IOException("Couldn't create folders " + root);
      }
      return result;
    }
  }

  class DrillTestCaseLoaderRule implements TestRule
  {

    @Override
    public Statement apply(Statement base, Description description)
    {
      DrillTest annotation = description.getAnnotation(DrillTest.class);
      testCase = (annotation == null) ? null : new DrillTestCase(annotation.value());
      return base;
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
      String subFolder = String.join("/", subDirs);
      File result = new File(root, subFolder);
      if (!result.mkdirs()) {
        throw new IOException("Couldn't create folders " + root);
      }
      return result;
    }
  }

  static class DrillTestCase
  {
    private final String query;
    private final List<String[]> results;
    private String filename;

    public DrillTestCase(String filename)
    {
      try {
        this.filename = filename;
        this.query = readStringFromResource(".q");
        String resultsStr = readStringFromResource(".e");
        String[] lines = resultsStr.split("\n");
        results = new ArrayList<>();
        if (resultsStr.length() > 0) {
          for (String string : lines) {
            String[] cols = string.split("\t");
            results.add(cols);
          }
        }
      }
      catch (Exception e) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Encountered exception while loading testcase [%s]", filename),
            e);
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

    @Nonnull
    private String readStringFromResource(String s) throws IOException
    {
      final String query;
      try (InputStream queryIn = ClassLoader.getSystemResourceAsStream("drill/window/queries/" + filename + s)) {
        query = new String(ByteStreams.toByteArray(queryIn), StandardCharsets.UTF_8);
      }
      return query;
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
      String subFolder = String.join("/", subDirs);
      File result = new File(root, subFolder);
      if (!result.mkdirs()) {
        throw new IOException("Couldn't create folders " + root);
      }
      return result;
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
    attachIndex(
        retVal,
        "smlTbl.parquet",
        // "col_int": 8122,
        new LongDimensionSchema("col_int"),
        // "col_bgint": 817200,
        new LongDimensionSchema("col_bgint"),
        // "col_char_2": "IN",
        new StringDimensionSchema("col_char_2"),
        // "col_vchar_52":
        // "AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB",
        new StringDimensionSchema("col_vchar_52"),
        // "col_tmstmp": 1409617682418,
        new LongDimensionSchema("col_tmstmp"),
        // "col_dt": 422717616000000,
        new LongDimensionSchema("col_dt"),
        // "col_booln": false,
        new StringDimensionSchema("col_booln"),
        // "col_dbl": 12900.48,
        new DoubleDimensionSchema("col_dbl"),
        // "col_tm": 33109170
        new LongDimensionSchema("col_tm"));
    attachIndex(
        retVal,
        "fewRowsAllData.parquet",
        // "col0":12024,
        new LongDimensionSchema("col0"),
        // "col1":307168,
        new LongDimensionSchema("col1"),
        // "col2":"VT",
        new StringDimensionSchema("col2"),
        // "col3":"DXXXXXXXXXXXXXXXXXXXXXXXXXEXXXXXXXXXXXXXXXXXXXXXXXXF",
        new StringDimensionSchema("col3"),
        // "col4":1338596882419,
        new LongDimensionSchema("col4"),
        // "col5":422705433600000,
        new LongDimensionSchema("col5"),
        // "col6":true,
        new StringDimensionSchema("col6"),
        // "col7":3.95110006277E8,
        new DoubleDimensionSchema("col7"),
        // "col8":67465430
        new LongDimensionSchema("col8"));
    attachIndex(
        retVal,
        "t_alltype.parquet",
        // "c1":1,
        new LongDimensionSchema("c1"),
        // "c2":592475043,
        new LongDimensionSchema("c2"),
        // "c3":616080519999272,
        new LongDimensionSchema("c3"),
        // "c4":"ObHeWTDEcbGzssDwPwurfs",
        new StringDimensionSchema("c4"),
        // "c5":"0sZxIfZ CGwTOaLWZ6nWkUNx",
        new StringDimensionSchema("c5"),
        // "c6":1456290852307,
        new LongDimensionSchema("c6"),
        // "c7":421426627200000,
        new LongDimensionSchema("c7"),
        // "c8":true,
        new StringDimensionSchema("c8"),
        // "c9":0.626179100469
        new DoubleDimensionSchema("c9"));

    return retVal;
  }

  public class TextualResultsVerifier implements ResultsVerifier
  {
    protected final List<String[]> expectedResultsText;
    @Nullable
    protected final RowSignature expectedResultRowSignature;
    private RowSignature currentRowSignature;

    public TextualResultsVerifier(List<String[]> expectedResultsString, RowSignature expectedSignature)
    {
      this.expectedResultsText = expectedResultsString;
      this.expectedResultRowSignature = expectedSignature;
    }

    @Override
    public void verifyRowSignature(RowSignature rowSignature)
    {
      if (expectedResultRowSignature != null) {
        assertEquals(expectedResultRowSignature, rowSignature);
      }
      currentRowSignature = rowSignature;
    }

    @Override
    public void verify(String sql, QueryResults queryResults)
    {
      List<Object[]> results = queryResults.results;
      List<Object[]> expectedResults = parseResults(currentRowSignature, expectedResultsText);
      try {
        assertEquals(expectedResultsText.size(), results.size(), StringUtils.format("result count: %s", sql));
        if (!isOrdered(queryResults)) {
          // in case the resultset is not ordered; order via the same comparator before comparision
          results.sort(new ArrayRowCmp());
          expectedResults.sort(new ArrayRowCmp());
        }
        assertResultsValid(ResultMatchMode.EQUALS_RELATIVE_1000_ULPS, expectedResults, queryResults);
      }
      catch (AssertionError e) {
        log.info("query: %s", sql);
        log.info(resultsToString("Expected", expectedResults));
        log.info(resultsToString("Actual", results));
        throw new AssertionError(StringUtils.format("%s while processing: %s", e.getMessage(), sql), e);
      }
    }

    private boolean isOrdered(QueryResults queryResults)
    {
      SqlNode sqlNode = queryResults.capture.getSqlNode();
      return SqlToRelConverter.isOrdered(sqlNode);
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
      String subFolder = String.join("/", subDirs);
      File result = new File(root, subFolder);
      if (!result.mkdirs()) {
        throw new IOException("Couldn't create folders " + root);
      }
      return result;
    }
  }

  static class ArrayRowCmp implements Comparator<Object[]>
  {
    @Override
    public int compare(Object[] arg0, Object[] arg1)
    {
      String s0 = Arrays.toString(arg0);
      String s1 = Arrays.toString(arg1);
      return s0.compareTo(s1);
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
      String subFolder = String.join("/", subDirs);
      File result = new File(root, subFolder);
      if (!result.mkdirs()) {
        throw new IOException("Couldn't create folders " + root);
      }
      return result;
    }
  }

  private static List<Object[]> parseResults(RowSignature rs, List<String[]> results)
  {
    List<Object[]> ret = new ArrayList<>();
    for (String[] row : results) {
      Object[] newRow = new Object[row.length];
      List<String> cc = rs.getColumnNames();
      for (int i = 0; i < cc.size(); i++) {
        ColumnType type = rs.getColumnType(i).get();
        assertNull(type.getComplexTypeName());
        final String val = row[i];
        Object newVal;
        if ("null".equals(val)) {
          newVal = null;
        } else {
          switch (type.getType()) {
            case STRING:
              newVal = val;
              break;
            case LONG:
              newVal = parseLongValue(val);
              break;
            case DOUBLE:
              newVal = Numbers.parseDoubleObject(val);
              break;
            default:
              throw new RuntimeException("unimplemented");
          }
        }
        newRow[i] = newVal;
      }
      ret.add(newRow);
    }
    return ret;
  }

  private static Object parseLongValue(final String val)
  {
    if ("".equals(val)) {
      return null;
    }
    try {
      return Long.parseLong(val);
    }
    catch (NumberFormatException e) {
    }
    try {
      double d = Double.parseDouble(val);
      return (long) d;
    }
    catch (NumberFormatException e) {
    }
    try {
      LocalTime v = LocalTime.parse(val);
      Long l = (long) v.getMillisOfDay();
      return l;
    }
    catch (Exception e) {
    }
    Function<String, DateTime> parser = TimestampParser.createTimestampParser("auto");
    try {
      DateTime v = parser.apply(val);
      return v.getMillis();
    }
    catch (IllegalArgumentException iae) {
    }
    throw new RuntimeException("Can't parse input!");
  }

  public void windowQueryTest()
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
              PlannerCaptureHook.NEED_CAPTURE_HOOK, true,
              QueryContexts.ENABLE_DEBUG, true)
              )
          .sql(testCase.getQueryString())
          .expectedResults(new TextualResultsVerifier(testCase.getExpectedResults(), null))
          .run();
    }
    finally {
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

    final File tmpFolder = newFolder(temporaryFolder, "junit");
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
              }
              catch (IOException e) {
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

  // testcases_start
  @DrillTest("aggregates/aggOWnFn_11")
  @Test
  void aggregates_agg_own_fn_11()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_13")
  @Test
  void aggregates_agg_own_fn_13()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_16")
  @Test
  void aggregates_agg_own_fn_16()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_1")
  @Test
  void aggregates_agg_own_fn_1()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_20")
  @Test
  void aggregates_agg_own_fn_20()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_21")
  @Test
  void aggregates_agg_own_fn_21()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_2")
  @Test
  void aggregates_agg_own_fn_2()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_30")
  @Test
  void aggregates_agg_own_fn_30()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_31")
  @Test
  void aggregates_agg_own_fn_31()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_32")
  @Test
  void aggregates_agg_own_fn_32()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_33")
  @Test
  void aggregates_agg_own_fn_33()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_34")
  @Test
  void aggregates_agg_own_fn_34()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_35")
  @Test
  void aggregates_agg_own_fn_35()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_36")
  @Test
  void aggregates_agg_own_fn_36()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_37")
  @Test
  void aggregates_agg_own_fn_37()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_38")
  @Test
  void aggregates_agg_own_fn_38()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_41")
  @Test
  void aggregates_agg_own_fn_41()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_42")
  @Test
  void aggregates_agg_own_fn_42()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_43")
  @Test
  void aggregates_agg_own_fn_43()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_44")
  @Test
  void aggregates_agg_own_fn_44()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_45")
  @Test
  void aggregates_agg_own_fn_45()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_46")
  @Test
  void aggregates_agg_own_fn_46()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_47")
  @Test
  void aggregates_agg_own_fn_47()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_48")
  @Test
  void aggregates_agg_own_fn_48()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_49")
  @Test
  void aggregates_agg_own_fn_49()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_51")
  @Test
  void aggregates_agg_own_fn_51()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_5")
  @Test
  void aggregates_agg_own_fn_5()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_6")
  @Test
  void aggregates_agg_own_fn_6()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_7")
  @Test
  void aggregates_agg_own_fn_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_8")
  @Test
  void aggregates_agg_own_fn_8()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_9")
  @Test
  void aggregates_agg_own_fn_9()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_01")
  @Test
  void aggregates_mty_ovr_cluse_01()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_02")
  @Test
  void aggregates_mty_ovr_cluse_02()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_03")
  @Test
  void aggregates_mty_ovr_cluse_03()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_04")
  @Test
  void aggregates_mty_ovr_cluse_04()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_05")
  @Test
  void aggregates_mty_ovr_cluse_05()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_11")
  @Test
  void aggregates_win_fn_qry_11()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_14")
  @Test
  void aggregates_win_fn_qry_14()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_19")
  @Test
  void aggregates_win_fn_qry_19()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_22")
  @Test
  void aggregates_win_fn_qry_22()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_24")
  @Test
  void aggregates_win_fn_qry_24()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_29")
  @Test
  void aggregates_win_fn_qry_29()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_56")
  @Test
  void aggregates_win_fn_qry_56()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_57")
  @Test
  void aggregates_win_fn_qry_57()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_58")
  @Test
  void aggregates_win_fn_qry_58()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_59")
  @Test
  void aggregates_win_fn_qry_59()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_60")
  @Test
  void aggregates_win_fn_qry_60()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_10")
  @Test
  void aggregates_wo_ordr_by_10()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_11")
  @Test
  void aggregates_wo_ordr_by_11()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_12")
  @Test
  void aggregates_wo_ordr_by_12()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_13")
  @Test
  void aggregates_wo_ordr_by_13()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_14")
  @Test
  void aggregates_wo_ordr_by_14()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_15")
  @Test
  void aggregates_wo_ordr_by_15()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_16")
  @Test
  void aggregates_wo_ordr_by_16()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_1")
  @Test
  void aggregates_wo_ordr_by_1()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_2")
  @Test
  void aggregates_wo_ordr_by_2()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_3")
  @Test
  void aggregates_wo_ordr_by_3()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_4")
  @Test
  void aggregates_wo_ordr_by_4()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_5")
  @Test
  void aggregates_wo_ordr_by_5()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_6")
  @Test
  void aggregates_wo_ordr_by_6()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_7")
  @Test
  void aggregates_wo_ordr_by_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_8")
  @Test
  void aggregates_wo_ordr_by_8()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_9")
  @Test
  void aggregates_wo_ordr_by_9()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_11")
  @Test
  void aggregates_wo_prtn_by_11()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_16")
  @Test
  void aggregates_wo_prtn_by_16()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_1")
  @Test
  void aggregates_wo_prtn_by_1()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_21")
  @Test
  void aggregates_wo_prtn_by_21()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_22")
  @Test
  void aggregates_wo_prtn_by_22()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_23")
  @Test
  void aggregates_wo_prtn_by_23()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_24")
  @Test
  void aggregates_wo_prtn_by_24()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_26")
  @Test
  void aggregates_wo_prtn_by_26()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_27")
  @Test
  void aggregates_wo_prtn_by_27()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_28")
  @Test
  void aggregates_wo_prtn_by_28()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_29")
  @Test
  void aggregates_wo_prtn_by_29()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_2")
  @Test
  void aggregates_wo_prtn_by_2()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_30")
  @Test
  void aggregates_wo_prtn_by_30()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_31")
  @Test
  void aggregates_wo_prtn_by_31()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_32")
  @Test
  void aggregates_wo_prtn_by_32()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_33")
  @Test
  void aggregates_wo_prtn_by_33()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_34")
  @Test
  void aggregates_wo_prtn_by_34()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_35")
  @Test
  void aggregates_wo_prtn_by_35()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_36")
  @Test
  void aggregates_wo_prtn_by_36()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_37")
  @Test
  void aggregates_wo_prtn_by_37()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_38")
  @Test
  void aggregates_wo_prtn_by_38()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_39")
  @Test
  void aggregates_wo_prtn_by_39()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_40")
  @Test
  void aggregates_wo_prtn_by_40()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_41")
  @Test
  void aggregates_wo_prtn_by_41()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_42")
  @Test
  void aggregates_wo_prtn_by_42()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_43")
  @Test
  void aggregates_wo_prtn_by_43()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_44")
  @Test
  void aggregates_wo_prtn_by_44()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_45")
  @Test
  void aggregates_wo_prtn_by_45()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_46")
  @Test
  void aggregates_wo_prtn_by_46()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_47")
  @Test
  void aggregates_wo_prtn_by_47()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_48")
  @Test
  void aggregates_wo_prtn_by_48()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_49")
  @Test
  void aggregates_wo_prtn_by_49()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_50")
  @Test
  void aggregates_wo_prtn_by_50()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_10")
  @Test
  void aggregates_w_prtn_ordr_by_10()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_1")
  @Test
  void aggregates_w_prtn_ordr_by_1()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_2")
  @Test
  void aggregates_w_prtn_ordr_by_2()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_3")
  @Test
  void aggregates_w_prtn_ordr_by_3()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_4")
  @Test
  void aggregates_w_prtn_ordr_by_4()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_5")
  @Test
  void aggregates_w_prtn_ordr_by_5()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_6")
  @Test
  void aggregates_w_prtn_ordr_by_6()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_7")
  @Test
  void aggregates_w_prtn_ordr_by_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_8")
  @Test
  void aggregates_w_prtn_ordr_by_8()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_9")
  @Test
  void aggregates_w_prtn_ordr_by_9()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_10")
  @Test
  void first_val_first_val_fn_10()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_11")
  @Test
  void first_val_first_val_fn_11()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_12")
  @Test
  void first_val_first_val_fn_12()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_13")
  @Test
  void first_val_first_val_fn_13()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_14")
  @Test
  void first_val_first_val_fn_14()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_15")
  @Test
  void first_val_first_val_fn_15()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_16")
  @Test
  void first_val_first_val_fn_16()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_1")
  @Test
  void first_val_first_val_fn_1()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_20")
  @Test
  void first_val_first_val_fn_20()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_26")
  @Test
  void first_val_first_val_fn_26()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_27")
  @Test
  void first_val_first_val_fn_27()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_28")
  @Test
  void first_val_first_val_fn_28()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_2")
  @Test
  void first_val_first_val_fn_2()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_30")
  @Test
  void first_val_first_val_fn_30()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_31")
  @Test
  void first_val_first_val_fn_31()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_3")
  @Test
  void first_val_first_val_fn_3()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_4")
  @Test
  void first_val_first_val_fn_4()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_6")
  @Test
  void first_val_first_val_fn_6()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_7")
  @Test
  void first_val_first_val_fn_7()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_8")
  @Test
  void first_val_first_val_fn_8()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_9")
  @Test
  void first_val_first_val_fn_9()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_5")
  @Test
  void frameclause_default_frame_rbupacr_bgint_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bln_1")
  @Test
  void frameclause_default_frame_rbupacr_bln_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bln_2")
  @Test
  void frameclause_default_frame_rbupacr_bln_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bln_3")
  @Test
  void frameclause_default_frame_rbupacr_bln_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_4")
  @Test
  void frameclause_default_frame_rbupacr_chr_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_4")
  @Test
  void frameclause_default_frame_rbupacr_dbl_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_5")
  @Test
  void frameclause_default_frame_rbupacr_dbl_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_3")
  @Test
  void frameclause_default_frame_rbupacr_dt_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int11")
  @Test
  void frameclause_default_frame_rbupacr_int11()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int12")
  @Test
  void frameclause_default_frame_rbupacr_int12()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int1")
  @Test
  void frameclause_default_frame_rbupacr_int1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int2")
  @Test
  void frameclause_default_frame_rbupacr_int2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int3")
  @Test
  void frameclause_default_frame_rbupacr_int3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int4")
  @Test
  void frameclause_default_frame_rbupacr_int4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int5")
  @Test
  void frameclause_default_frame_rbupacr_int5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int6")
  @Test
  void frameclause_default_frame_rbupacr_int6()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_4")
  @Test
  void frameclause_default_frame_rbupacr_vchr_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_1")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_2")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_3")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_4")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_5")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bln_1")
  @Test
  void frameclause_rbcracr_rbcracr_bln_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bln_2")
  @Test
  void frameclause_rbcracr_rbcracr_bln_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bln_3")
  @Test
  void frameclause_rbcracr_rbcracr_bln_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_1")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_2")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_3")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_4")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_5")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_3")
  @Test
  void frameclause_rbcracr_rbcracr_dt_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_10")
  @Test
  void frameclause_rbcracr_rbcracr_int_10()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_11")
  @Test
  void frameclause_rbcracr_rbcracr_int_11()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_12")
  @Test
  void frameclause_rbcracr_rbcracr_int_12()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_1")
  @Test
  void frameclause_rbcracr_rbcracr_int_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_2")
  @Test
  void frameclause_rbcracr_rbcracr_int_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_3")
  @Test
  void frameclause_rbcracr_rbcracr_int_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_4")
  @Test
  void frameclause_rbcracr_rbcracr_int_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_5")
  @Test
  void frameclause_rbcracr_rbcracr_int_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_8")
  @Test
  void frameclause_rbcracr_rbcracr_int_8()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_9")
  @Test
  void frameclause_rbcracr_rbcracr_int_9()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_5")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bln_1")
  @Test
  void frameclause_rbupacr_rbupacr_bln_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bln_2")
  @Test
  void frameclause_rbupacr_rbupacr_bln_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bln_3")
  @Test
  void frameclause_rbupacr_rbupacr_bln_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_4")
  @Test
  void frameclause_rbupacr_rbupacr_chr_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_4")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_5")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_3")
  @Test
  void frameclause_rbupacr_rbupacr_dt_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int11")
  @Test
  void frameclause_rbupacr_rbupacr_int11()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int12")
  @Test
  void frameclause_rbupacr_rbupacr_int12()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int1")
  @Test
  void frameclause_rbupacr_rbupacr_int1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int2")
  @Test
  void frameclause_rbupacr_rbupacr_int2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int3")
  @Test
  void frameclause_rbupacr_rbupacr_int3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int4")
  @Test
  void frameclause_rbupacr_rbupacr_int4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int5")
  @Test
  void frameclause_rbupacr_rbupacr_int5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int6")
  @Test
  void frameclause_rbupacr_rbupacr_int6()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_4")
  @Test
  void frameclause_rbupacr_rbupacr_vchr_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_1")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_2")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_3")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_5")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_7")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bln_1")
  @Test
  void frameclause_rbupauf_rbupauf_bln_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bln_2")
  @Test
  void frameclause_rbupauf_rbupauf_bln_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bln_3")
  @Test
  void frameclause_rbupauf_rbupauf_bln_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_4")
  @Test
  void frameclause_rbupauf_rbupauf_char_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_5")
  @Test
  void frameclause_rbupauf_rbupauf_char_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_1")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_2")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_3")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_4")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_5")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_3")
  @Test
  void frameclause_rbupauf_rbupauf_dt_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_10")
  @Test
  void frameclause_rbupauf_rbupauf_int_10()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_11")
  @Test
  void frameclause_rbupauf_rbupauf_int_11()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_12")
  @Test
  void frameclause_rbupauf_rbupauf_int_12()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_14")
  @Test
  void frameclause_rbupauf_rbupauf_int_14()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_1")
  @Test
  void frameclause_rbupauf_rbupauf_int_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_2")
  @Test
  void frameclause_rbupauf_rbupauf_int_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_3")
  @Test
  void frameclause_rbupauf_rbupauf_int_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_4")
  @Test
  void frameclause_rbupauf_rbupauf_int_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_5")
  @Test
  void frameclause_rbupauf_rbupauf_int_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_6")
  @Test
  void frameclause_rbupauf_rbupauf_int_6()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_7")
  @Test
  void frameclause_rbupauf_rbupauf_int_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_8")
  @Test
  void frameclause_rbupauf_rbupauf_int_8()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_9")
  @Test
  void frameclause_rbupauf_rbupauf_int_9()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_4")
  @Test
  void frameclause_rbupauf_rbupauf_vchar_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_5")
  @Test
  void frameclause_rbupauf_rbupauf_vchar_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_01")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_01()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_02")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_02()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_03")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_03()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_04")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_04()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_05")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_05()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_06")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_06()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_07")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_07()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_08")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_08()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_09")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_09()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_10")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_10()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_11")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_11()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_12")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_12()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_13")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_13()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_14")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_14()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_15")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_15()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_16")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_16()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_18")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_18()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_19")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_19()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_21")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_21()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_29")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_29()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_31")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_31()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_32")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_32()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_33")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_33()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_34")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_34()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_35")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_35()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_36")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_36()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_37")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_37()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_38")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_38()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_39")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_39()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_40")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_40()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_50")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_50()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_51")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_51()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_52")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_52()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_56")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_56()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_104")
  @Test
  void lag_func_lag_fn_104()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_105")
  @Test
  void lag_func_lag_fn_105()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_106")
  @Test
  void lag_func_lag_fn_106()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_107")
  @Test
  void lag_func_lag_fn_107()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_110")
  @Test
  void lag_func_lag_fn_110()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_111")
  @Test
  void lag_func_lag_fn_111()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_112")
  @Test
  void lag_func_lag_fn_112()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_1")
  @Test
  void lag_func_lag_fn_1()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_28")
  @Test
  void lag_func_lag_fn_28()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_29")
  @Test
  void lag_func_lag_fn_29()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_2")
  @Test
  void lag_func_lag_fn_2()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_30")
  @Test
  void lag_func_lag_fn_30()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_31")
  @Test
  void lag_func_lag_fn_31()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_32")
  @Test
  void lag_func_lag_fn_32()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_34")
  @Test
  void lag_func_lag_fn_34()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_35")
  @Test
  void lag_func_lag_fn_35()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_37")
  @Test
  void lag_func_lag_fn_37()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_38")
  @Test
  void lag_func_lag_fn_38()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_39")
  @Test
  void lag_func_lag_fn_39()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_3")
  @Test
  void lag_func_lag_fn_3()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_40")
  @Test
  void lag_func_lag_fn_40()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_43")
  @Test
  void lag_func_lag_fn_43()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_44")
  @Test
  void lag_func_lag_fn_44()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_46")
  @Test
  void lag_func_lag_fn_46()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_47")
  @Test
  void lag_func_lag_fn_47()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_48")
  @Test
  void lag_func_lag_fn_48()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_49")
  @Test
  void lag_func_lag_fn_49()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_4")
  @Test
  void lag_func_lag_fn_4()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_50")
  @Test
  void lag_func_lag_fn_50()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_52")
  @Test
  void lag_func_lag_fn_52()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_53")
  @Test
  void lag_func_lag_fn_53()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_55")
  @Test
  void lag_func_lag_fn_55()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_56")
  @Test
  void lag_func_lag_fn_56()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_57")
  @Test
  void lag_func_lag_fn_57()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_58")
  @Test
  void lag_func_lag_fn_58()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_59")
  @Test
  void lag_func_lag_fn_59()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_5")
  @Test
  void lag_func_lag_fn_5()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_61")
  @Test
  void lag_func_lag_fn_61()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_62")
  @Test
  void lag_func_lag_fn_62()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_70")
  @Test
  void lag_func_lag_fn_70()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_73")
  @Test
  void lag_func_lag_fn_73()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_74")
  @Test
  void lag_func_lag_fn_74()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_75")
  @Test
  void lag_func_lag_fn_75()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_76")
  @Test
  void lag_func_lag_fn_76()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_78")
  @Test
  void lag_func_lag_fn_78()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_79")
  @Test
  void lag_func_lag_fn_79()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_7")
  @Test
  void lag_func_lag_fn_7()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_80")
  @Test
  void lag_func_lag_fn_80()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_81")
  @Test
  void lag_func_lag_fn_81()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_83")
  @Test
  void lag_func_lag_fn_83()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_84")
  @Test
  void lag_func_lag_fn_84()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_85")
  @Test
  void lag_func_lag_fn_85()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_86")
  @Test
  void lag_func_lag_fn_86()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_87")
  @Test
  void lag_func_lag_fn_87()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_88")
  @Test
  void lag_func_lag_fn_88()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_89")
  @Test
  void lag_func_lag_fn_89()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_8")
  @Test
  void lag_func_lag_fn_8()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_90")
  @Test
  void lag_func_lag_fn_90()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_91")
  @Test
  void lag_func_lag_fn_91()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_92")
  @Test
  void lag_func_lag_fn_92()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_93")
  @Test
  void lag_func_lag_fn_93()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_94")
  @Test
  void lag_func_lag_fn_94()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_98")
  @Test
  void lag_func_lag_fn_98()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_26")
  @Test
  void last_val_last_val_fn_26()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_27")
  @Test
  void last_val_last_val_fn_27()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_28")
  @Test
  void last_val_last_val_fn_28()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_30")
  @Test
  void last_val_last_val_fn_30()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_31")
  @Test
  void last_val_last_val_fn_31()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_37")
  @Test
  void last_val_last_val_fn_37()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_100")
  @Test
  void lead_func_lead_fn_100()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_101")
  @Test
  void lead_func_lead_fn_101()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_102")
  @Test
  void lead_func_lead_fn_102()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_105")
  @Test
  void lead_func_lead_fn_105()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_106")
  @Test
  void lead_func_lead_fn_106()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_107")
  @Test
  void lead_func_lead_fn_107()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_1")
  @Test
  void lead_func_lead_fn_1()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_28")
  @Test
  void lead_func_lead_fn_28()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_29")
  @Test
  void lead_func_lead_fn_29()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_2")
  @Test
  void lead_func_lead_fn_2()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_30")
  @Test
  void lead_func_lead_fn_30()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_31")
  @Test
  void lead_func_lead_fn_31()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_32")
  @Test
  void lead_func_lead_fn_32()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_34")
  @Test
  void lead_func_lead_fn_34()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_35")
  @Test
  void lead_func_lead_fn_35()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_37")
  @Test
  void lead_func_lead_fn_37()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_38")
  @Test
  void lead_func_lead_fn_38()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_39")
  @Test
  void lead_func_lead_fn_39()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_3")
  @Test
  void lead_func_lead_fn_3()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_40")
  @Test
  void lead_func_lead_fn_40()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_41")
  @Test
  void lead_func_lead_fn_41()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_43")
  @Test
  void lead_func_lead_fn_43()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_44")
  @Test
  void lead_func_lead_fn_44()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_46")
  @Test
  void lead_func_lead_fn_46()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_47")
  @Test
  void lead_func_lead_fn_47()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_48")
  @Test
  void lead_func_lead_fn_48()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_49")
  @Test
  void lead_func_lead_fn_49()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_4")
  @Test
  void lead_func_lead_fn_4()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_50")
  @Test
  void lead_func_lead_fn_50()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_52")
  @Test
  void lead_func_lead_fn_52()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_53")
  @Test
  void lead_func_lead_fn_53()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_55")
  @Test
  void lead_func_lead_fn_55()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_56")
  @Test
  void lead_func_lead_fn_56()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_57")
  @Test
  void lead_func_lead_fn_57()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_58")
  @Test
  void lead_func_lead_fn_58()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_59")
  @Test
  void lead_func_lead_fn_59()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_5")
  @Test
  void lead_func_lead_fn_5()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_61")
  @Test
  void lead_func_lead_fn_61()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_62")
  @Test
  void lead_func_lead_fn_62()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_70")
  @Test
  void lead_func_lead_fn_70()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_73")
  @Test
  void lead_func_lead_fn_73()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_74")
  @Test
  void lead_func_lead_fn_74()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_75")
  @Test
  void lead_func_lead_fn_75()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_76")
  @Test
  void lead_func_lead_fn_76()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_78")
  @Test
  void lead_func_lead_fn_78()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_79")
  @Test
  void lead_func_lead_fn_79()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_7")
  @Test
  void lead_func_lead_fn_7()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_80")
  @Test
  void lead_func_lead_fn_80()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_81")
  @Test
  void lead_func_lead_fn_81()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_82")
  @Test
  void lead_func_lead_fn_82()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_83")
  @Test
  void lead_func_lead_fn_83()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_84")
  @Test
  void lead_func_lead_fn_84()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_85")
  @Test
  void lead_func_lead_fn_85()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_86")
  @Test
  void lead_func_lead_fn_86()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_87")
  @Test
  void lead_func_lead_fn_87()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_88")
  @Test
  void lead_func_lead_fn_88()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_89")
  @Test
  void lead_func_lead_fn_89()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_8")
  @Test
  void lead_func_lead_fn_8()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_93")
  @Test
  void lead_func_lead_fn_93()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_99")
  @Test
  void lead_func_lead_fn_99()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_1")
  @Test
  void nested_aggs_basic_1()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_2")
  @Test
  void nested_aggs_basic_2()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_4")
  @Test
  void nested_aggs_basic_4()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_6")
  @Test
  void nested_aggs_basic_6()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_8")
  @Test
  void nested_aggs_basic_8()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_9")
  @Test
  void nested_aggs_basic_9()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_10")
  @Test
  void nested_aggs_emty_ovr_cls_10()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_11")
  @Test
  void nested_aggs_emty_ovr_cls_11()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_12")
  @Test
  void nested_aggs_emty_ovr_cls_12()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_1")
  @Test
  void nested_aggs_emty_ovr_cls_1()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_2")
  @Test
  void nested_aggs_emty_ovr_cls_2()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_3")
  @Test
  void nested_aggs_emty_ovr_cls_3()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_4")
  @Test
  void nested_aggs_emty_ovr_cls_4()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_5")
  @Test
  void nested_aggs_emty_ovr_cls_5()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_6")
  @Test
  void nested_aggs_emty_ovr_cls_6()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_9")
  @Test
  void nested_aggs_emty_ovr_cls_9()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause01")
  @Test
  void nested_aggs_frmclause01()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause02")
  @Test
  void nested_aggs_frmclause02()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause06")
  @Test
  void nested_aggs_frmclause06()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause14")
  @Test
  void nested_aggs_frmclause14()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause19")
  @Test
  void nested_aggs_frmclause19()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_1")
  @Test
  void nested_aggs_multi_win_1()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg01")
  @Test
  void nested_aggs_nstdagg01()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg02")
  @Test
  void nested_aggs_nstdagg02()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg03")
  @Test
  void nested_aggs_nstdagg03()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg04")
  @Test
  void nested_aggs_nstdagg04()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg05")
  @Test
  void nested_aggs_nstdagg05()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg06")
  @Test
  void nested_aggs_nstdagg06()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg07")
  @Test
  void nested_aggs_nstdagg07()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg08")
  @Test
  void nested_aggs_nstdagg08()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg09")
  @Test
  void nested_aggs_nstdagg09()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg10")
  @Test
  void nested_aggs_nstdagg10()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg11")
  @Test
  void nested_aggs_nstdagg11()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg12")
  @Test
  void nested_aggs_nstdagg12()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg13")
  @Test
  void nested_aggs_nstdagg13()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg14")
  @Test
  void nested_aggs_nstdagg14()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg15")
  @Test
  void nested_aggs_nstdagg15()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg16")
  @Test
  void nested_aggs_nstdagg16()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg17")
  @Test
  void nested_aggs_nstdagg17()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg18")
  @Test
  void nested_aggs_nstdagg18()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg19")
  @Test
  void nested_aggs_nstdagg19()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg20")
  @Test
  void nested_aggs_nstdagg20()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg21")
  @Test
  void nested_aggs_nstdagg21()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg22")
  @Test
  void nested_aggs_nstdagg22()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg23")
  @Test
  void nested_aggs_nstdagg23()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg25")
  @Test
  void nested_aggs_nstdagg25()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg26")
  @Test
  void nested_aggs_nstdagg26()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_1")
  @Test
  void nested_aggs_wout_oby_1()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_2")
  @Test
  void nested_aggs_wout_oby_2()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_3")
  @Test
  void nested_aggs_wout_oby_3()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_4")
  @Test
  void nested_aggs_wout_oby_4()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_5")
  @Test
  void nested_aggs_wout_oby_5()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_8")
  @Test
  void nested_aggs_wout_oby_8()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_10")
  @Test
  void nested_aggs_w_pb_ob_10()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_11")
  @Test
  void nested_aggs_w_pb_ob_11()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_12")
  @Test
  void nested_aggs_w_pb_ob_12()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_13")
  @Test
  void nested_aggs_w_pb_ob_13()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_14")
  @Test
  void nested_aggs_w_pb_ob_14()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_15")
  @Test
  void nested_aggs_w_pb_ob_15()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_16")
  @Test
  void nested_aggs_w_pb_ob_16()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_17")
  @Test
  void nested_aggs_w_pb_ob_17()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_18")
  @Test
  void nested_aggs_w_pb_ob_18()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_19")
  @Test
  void nested_aggs_w_pb_ob_19()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_1")
  @Test
  void nested_aggs_w_pb_ob_1()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_20")
  @Test
  void nested_aggs_w_pb_ob_20()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_21")
  @Test
  void nested_aggs_w_pb_ob_21()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_22")
  @Test
  void nested_aggs_w_pb_ob_22()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_23")
  @Test
  void nested_aggs_w_pb_ob_23()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_24")
  @Test
  void nested_aggs_w_pb_ob_24()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_25")
  @Test
  void nested_aggs_w_pb_ob_25()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_26")
  @Test
  void nested_aggs_w_pb_ob_26()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_2")
  @Test
  void nested_aggs_w_pb_ob_2()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_3")
  @Test
  void nested_aggs_w_pb_ob_3()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_4")
  @Test
  void nested_aggs_w_pb_ob_4()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_5")
  @Test
  void nested_aggs_w_pb_ob_5()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_6")
  @Test
  void nested_aggs_w_pb_ob_6()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_7")
  @Test
  void nested_aggs_w_pb_ob_7()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_8")
  @Test
  void nested_aggs_w_pb_ob_8()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_9")
  @Test
  void nested_aggs_w_pb_ob_9()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_10")
  @Test
  void ntile_func_ntile_fn_10()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_11")
  @Test
  void ntile_func_ntile_fn_11()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_12")
  @Test
  void ntile_func_ntile_fn_12()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_13")
  @Test
  void ntile_func_ntile_fn_13()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_14")
  @Test
  void ntile_func_ntile_fn_14()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_16")
  @Test
  void ntile_func_ntile_fn_16()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_18")
  @Test
  void ntile_func_ntile_fn_18()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_19")
  @Test
  void ntile_func_ntile_fn_19()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_1")
  @Test
  void ntile_func_ntile_fn_1()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_20")
  @Test
  void ntile_func_ntile_fn_20()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_21")
  @Test
  void ntile_func_ntile_fn_21()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_22")
  @Test
  void ntile_func_ntile_fn_22()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_23")
  @Test
  void ntile_func_ntile_fn_23()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_24")
  @Test
  void ntile_func_ntile_fn_24()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_25")
  @Test
  void ntile_func_ntile_fn_25()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_26")
  @Test
  void ntile_func_ntile_fn_26()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_28")
  @Test
  void ntile_func_ntile_fn_28()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_29")
  @Test
  void ntile_func_ntile_fn_29()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_2")
  @Test
  void ntile_func_ntile_fn_2()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_30")
  @Test
  void ntile_func_ntile_fn_30()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_31")
  @Test
  void ntile_func_ntile_fn_31()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_32")
  @Test
  void ntile_func_ntile_fn_32()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_35")
  @Test
  void ntile_func_ntile_fn_35()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_36")
  @Test
  void ntile_func_ntile_fn_36()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_37")
  @Test
  void ntile_func_ntile_fn_37()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_38")
  @Test
  void ntile_func_ntile_fn_38()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_39")
  @Test
  void ntile_func_ntile_fn_39()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_3")
  @Test
  void ntile_func_ntile_fn_3()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_40")
  @Test
  void ntile_func_ntile_fn_40()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_41")
  @Test
  void ntile_func_ntile_fn_41()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_42")
  @Test
  void ntile_func_ntile_fn_42()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_43")
  @Test
  void ntile_func_ntile_fn_43()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_44")
  @Test
  void ntile_func_ntile_fn_44()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_46")
  @Test
  void ntile_func_ntile_fn_46()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_4")
  @Test
  void ntile_func_ntile_fn_4()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_5")
  @Test
  void ntile_func_ntile_fn_5()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_6")
  @Test
  void ntile_func_ntile_fn_6()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_8")
  @Test
  void ntile_func_ntile_fn_8()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_9")
  @Test
  void ntile_func_ntile_fn_9()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/cte_win_02")
  @Test
  void nested_aggs_cte_win_02()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/cte_win_03")
  @Test
  void nested_aggs_cte_win_03()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/cte_win_04")
  @Test
  void nested_aggs_cte_win_04()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause04")
  @Test
  void nested_aggs_frmclause04()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause05")
  @Test
  void nested_aggs_frmclause05()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause07")
  @Test
  void nested_aggs_frmclause07()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause08")
  @Test
  void nested_aggs_frmclause08()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause09")
  @Test
  void nested_aggs_frmclause09()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause10")
  @Test
  void nested_aggs_frmclause10()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause11")
  @Test
  void nested_aggs_frmclause11()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause13")
  @Test
  void nested_aggs_frmclause13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause15")
  @Test
  void nested_aggs_frmclause15()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause17")
  @Test
  void nested_aggs_frmclause17()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause18")
  @Test
  void nested_aggs_frmclause18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_10")
  @Test
  void nested_aggs_wout_oby_10()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_11")
  @Test
  void nested_aggs_wout_oby_11()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_12")
  @Test
  void nested_aggs_wout_oby_12()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_13")
  @Test
  void nested_aggs_wout_oby_13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_6")
  @Test
  void nested_aggs_wout_oby_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_7")
  @Test
  void nested_aggs_wout_oby_7()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_9")
  @Test
  void nested_aggs_wout_oby_9()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutPrtnBy_6")
  @Test
  void nested_aggs_wout_prtn_by_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutPrtnBy_7")
  @Test
  void nested_aggs_wout_prtn_by_7()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.ALLDATA_CSV)
  @DrillTest("aggregates/winFnQry_17")
  @Test
  void aggregates_win_fn_qry_17()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TIME_COMPARE)
  @DrillTest("lead_func/lead_Fn_27")
  @Test
  void lead_func_lead_fn_27()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_15")
  @Test
  void aggregates_win_fn_qry_15()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_23")
  @Test
  void aggregates_win_fn_qry_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_32")
  @Test
  void aggregates_win_fn_qry_32()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_33")
  @Test
  void aggregates_win_fn_qry_33()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_34")
  @Test
  void aggregates_win_fn_qry_34()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_35")
  @Test
  void aggregates_win_fn_qry_35()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_36")
  @Test
  void aggregates_win_fn_qry_36()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_37")
  @Test
  void aggregates_win_fn_qry_37()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_38")
  @Test
  void aggregates_win_fn_qry_38()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_39")
  @Test
  void aggregates_win_fn_qry_39()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_40")
  @Test
  void aggregates_win_fn_qry_40()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_41")
  @Test
  void aggregates_win_fn_qry_41()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_42")
  @Test
  void aggregates_win_fn_qry_42()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_43")
  @Test
  void aggregates_win_fn_qry_43()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_44")
  @Test
  void aggregates_win_fn_qry_44()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_45")
  @Test
  void aggregates_win_fn_qry_45()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_6")
  @Test
  void aggregates_win_fn_qry_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_9")
  @Test
  void aggregates_win_fn_qry_9()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_5")
  @Test
  void nested_aggs_multi_win_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.COLUMN_NOT_FOUND)
  @DrillTest("frameclause/subQueries/frmInSubQry_25")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_25()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.INCORRECT_SYNTAX)
  @DrillTest("nestedAggs/nstdWinView01")
  @Test
  void nested_aggs_nstd_win_view01()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_63")
  @Test
  void aggregates_win_fn_qry_63()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/winFnQry_83")
  @Test
  void aggregates_win_fn_qry_83()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("frameclause/multipl_wnwds/mulwind_01")
  @Test
  void frameclause_multipl_wnwds_mulwind_01()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("frameclause/multipl_wnwds/mulwind_06")
  @Test
  void frameclause_multipl_wnwds_mulwind_06()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("frameclause/multipl_wnwds/mulwind_07")
  @Test
  void frameclause_multipl_wnwds_mulwind_07()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_108")
  @Test
  void lag_func_lag_fn_108()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_109")
  @Test
  void lag_func_lag_fn_109()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_69")
  @Test
  void lag_func_lag_fn_69()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_103")
  @Test
  void lead_func_lead_fn_103()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_104")
  @Test
  void lead_func_lead_fn_104()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_69")
  @Test
  void lead_func_lead_fn_69()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("nestedAggs/multiWin_7")
  @Test
  void nested_aggs_multi_win_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_3")
  @Test
  void aggregates_agg_own_fn_3()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_4")
  @Test
  void aggregates_agg_own_fn_4()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_29")
  @Test
  void first_val_first_val_fn_29()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_32")
  @Test
  void first_val_first_val_fn_32()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("first_val/firstValFn_33")
  @Test
  void first_val_first_val_fn_33()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_int7")
  @Test
  void frameclause_default_frame_rbupacr_int7()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_9")
  @Test
  void lag_func_lag_fn_9()
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_29")
  @Test
  void last_val_last_val_fn_29()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_34")
  @Test
  void last_val_last_val_fn_34()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_35")
  @Test
  void last_val_last_val_fn_35()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_38")
  @Test
  void last_val_last_val_fn_38()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_39")
  @Test
  void last_val_last_val_fn_39()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NOT_ENOUGH_RULES)
  @DrillTest("nestedAggs/emtyOvrCls_7")
  @Test
  void nested_aggs_emty_ovr_cls_7()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_33")
  @Test
  void ntile_func_ntile_fn_33()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_34")
  @Test
  void ntile_func_ntile_fn_34()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_47")
  @Test
  void ntile_func_ntile_fn_47()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_48")
  @Test
  void ntile_func_ntile_fn_48()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_49")
  @Test
  void ntile_func_ntile_fn_49()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_50")
  @Test
  void ntile_func_ntile_fn_50()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_51")
  @Test
  void ntile_func_ntile_fn_51()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_52")
  @Test
  void ntile_func_ntile_fn_52()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_53")
  @Test
  void ntile_func_ntile_fn_53()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_54")
  @Test
  void ntile_func_ntile_fn_54()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_55")
  @Test
  void ntile_func_ntile_fn_55()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_56")
  @Test
  void ntile_func_ntile_fn_56()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_57")
  @Test
  void ntile_func_ntile_fn_57()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_58")
  @Test
  void ntile_func_ntile_fn_58()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("aggregates/winFnQry_12")
  @Test
  void aggregates_win_fn_qry_12()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("aggregates/winFnQry_13")
  @Test
  void aggregates_win_fn_qry_13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("aggregates/winFnQry_20")
  @Test
  void aggregates_win_fn_qry_20()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("aggregates/winFnQry_21")
  @Test
  void aggregates_win_fn_qry_21()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NPE)
  @DrillTest("first_val/firstValFn_5")
  @Test
  void first_val_first_val_fn_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_1")
  @Test
  void frameclause_default_frame_rbupacr_chr_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_2")
  @Test
  void frameclause_default_frame_rbupacr_chr_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_1")
  @Test
  void frameclause_default_frame_rbupacr_vchr_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_2")
  @Test
  void frameclause_default_frame_rbupacr_vchr_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/multipl_wnwds/max_mulwds")
  @Test
  void frameclause_multipl_wnwds_max_mulwds()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/multipl_wnwds/min_mulwds")
  @Test
  void frameclause_multipl_wnwds_min_mulwds()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBCRACR/RBCRACR_char_1")
  @Test
  void frameclause_rbcracr_rbcracr_char_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBCRACR/RBCRACR_char_2")
  @Test
  void frameclause_rbcracr_rbcracr_char_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_1")
  @Test
  void frameclause_rbcracr_rbcracr_vchar_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_2")
  @Test
  void frameclause_rbcracr_rbcracr_vchar_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_1")
  @Test
  void frameclause_rbupacr_rbupacr_chr_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_2")
  @Test
  void frameclause_rbupacr_rbupacr_chr_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_1")
  @Test
  void frameclause_rbupacr_rbupacr_vchr_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_2")
  @Test
  void frameclause_rbupacr_rbupacr_vchr_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_1")
  @Test
  void frameclause_rbupauf_rbupauf_char_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_2")
  @Test
  void frameclause_rbupauf_rbupauf_char_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_1")
  @Test
  void frameclause_rbupauf_rbupauf_vchar_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_2")
  @Test
  void frameclause_rbupauf_rbupauf_vchar_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_22")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_23")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_24")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_41")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_41()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_42")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_42()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_43")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_43()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_44")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_44()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_45")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_45()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("frameclause/subQueries/frmInSubQry_46")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_46()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NPE)
  @DrillTest("lag_func/lag_Fn_82")
  @Test
  void lag_func_lag_fn_82()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NPE)
  @DrillTest("last_val/lastValFn_5")
  @Test
  void last_val_last_val_fn_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/basic_10")
  @Test
  void nested_aggs_basic_10()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/cte_win_01")
  @Test
  void nested_aggs_cte_win_01()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_7")
  @Test
  void aggregates_win_fn_qry_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_10")
  @Test
  void aggregates_test_w_nulls_10()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_11")
  @Test
  void aggregates_test_w_nulls_11()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_12")
  @Test
  void aggregates_test_w_nulls_12()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_13")
  @Test
  void aggregates_test_w_nulls_13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_14")
  @Test
  void aggregates_test_w_nulls_14()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_15")
  @Test
  void aggregates_test_w_nulls_15()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_16")
  @Test
  void aggregates_test_w_nulls_16()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_17")
  @Test
  void aggregates_test_w_nulls_17()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_18")
  @Test
  void aggregates_test_w_nulls_18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_19")
  @Test
  void aggregates_test_w_nulls_19()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_20")
  @Test
  void aggregates_test_w_nulls_20()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_21")
  @Test
  void aggregates_test_w_nulls_21()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_22")
  @Test
  void aggregates_test_w_nulls_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_23")
  @Test
  void aggregates_test_w_nulls_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_24")
  @Test
  void aggregates_test_w_nulls_24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_25")
  @Test
  void aggregates_test_w_nulls_25()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_26")
  @Test
  void aggregates_test_w_nulls_26()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_27")
  @Test
  void aggregates_test_w_nulls_27()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_28")
  @Test
  void aggregates_test_w_nulls_28()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_29")
  @Test
  void aggregates_test_w_nulls_29()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_2")
  @Test
  void aggregates_test_w_nulls_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_30")
  @Test
  void aggregates_test_w_nulls_30()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_31")
  @Test
  void aggregates_test_w_nulls_31()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_32")
  @Test
  void aggregates_test_w_nulls_32()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_33")
  @Test
  void aggregates_test_w_nulls_33()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_34")
  @Test
  void aggregates_test_w_nulls_34()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_35")
  @Test
  void aggregates_test_w_nulls_35()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_36")
  @Test
  void aggregates_test_w_nulls_36()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_37")
  @Test
  void aggregates_test_w_nulls_37()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_38")
  @Test
  void aggregates_test_w_nulls_38()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/testW_Nulls_39")
  @Test
  void aggregates_test_w_nulls_39()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_3")
  @Test
  void aggregates_test_w_nulls_3()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_4")
  @Test
  void aggregates_test_w_nulls_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.COLUMN_NOT_FOUND)
  @DrillTest("aggregates/testW_Nulls_5")
  @Test
  void aggregates_test_w_nulls_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.COLUMN_NOT_FOUND)
  @DrillTest("aggregates/testW_Nulls_6")
  @Test
  void aggregates_test_w_nulls_6()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_7")
  @Test
  void aggregates_test_w_nulls_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_8")
  @Test
  void aggregates_test_w_nulls_8()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_9")
  @Test
  void aggregates_test_w_nulls_9()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_61")
  @Test
  void aggregates_win_fn_qry_61()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_62")
  @Test
  void aggregates_win_fn_qry_62()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_64")
  @Test
  void aggregates_win_fn_qry_64()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_65")
  @Test
  void aggregates_win_fn_qry_65()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_66")
  @Test
  void aggregates_win_fn_qry_66()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_67")
  @Test
  void aggregates_win_fn_qry_67()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_68")
  @Test
  void aggregates_win_fn_qry_68()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_69")
  @Test
  void aggregates_win_fn_qry_69()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_70")
  @Test
  void aggregates_win_fn_qry_70()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_71")
  @Test
  void aggregates_win_fn_qry_71()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_72")
  @Test
  void aggregates_win_fn_qry_72()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_73")
  @Test
  void aggregates_win_fn_qry_73()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_74")
  @Test
  void aggregates_win_fn_qry_74()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_75")
  @Test
  void aggregates_win_fn_qry_75()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_76")
  @Test
  void aggregates_win_fn_qry_76()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_77")
  @Test
  void aggregates_win_fn_qry_77()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_78")
  @Test
  void aggregates_win_fn_qry_78()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_79")
  @Test
  void aggregates_win_fn_qry_79()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_80")
  @Test
  void aggregates_win_fn_qry_80()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_81")
  @Test
  void aggregates_win_fn_qry_81()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_82")
  @Test
  void aggregates_win_fn_qry_82()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_10")
  @Test
  void lag_func_lag_fn_10()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_11")
  @Test
  void lag_func_lag_fn_11()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_12")
  @Test
  void lag_func_lag_fn_12()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_13")
  @Test
  void lag_func_lag_fn_13()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_14")
  @Test
  void lag_func_lag_fn_14()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_15")
  @Test
  void lag_func_lag_fn_15()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_16")
  @Test
  void lag_func_lag_fn_16()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_17")
  @Test
  void lag_func_lag_fn_17()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_18")
  @Test
  void lag_func_lag_fn_18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_19")
  @Test
  void lag_func_lag_fn_19()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_20")
  @Test
  void lag_func_lag_fn_20()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_21")
  @Test
  void lag_func_lag_fn_21()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_22")
  @Test
  void lag_func_lag_fn_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_23")
  @Test
  void lag_func_lag_fn_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_24")
  @Test
  void lag_func_lag_fn_24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_25")
  @Test
  void lag_func_lag_fn_25()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_26")
  @Test
  void lag_func_lag_fn_26()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_54")
  @Test
  void lag_func_lag_fn_54()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_64")
  @Test
  void lag_func_lag_fn_64()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_65")
  @Test
  void lag_func_lag_fn_65()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_66")
  @Test
  void lag_func_lag_fn_66()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_67")
  @Test
  void lag_func_lag_fn_67()
  {
    windowQueryTest();

  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_68")
  @Test
  void lag_func_lag_fn_68()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_71")
  @Test
  void lag_func_lag_fn_71()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lag_func/lag_Fn_72")
  @Test
  void lag_func_lag_fn_72()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_10")
  @Test
  void lead_func_lead_fn_10()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_11")
  @Test
  void lead_func_lead_fn_11()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_12")
  @Test
  void lead_func_lead_fn_12()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_13")
  @Test
  void lead_func_lead_fn_13()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_14")
  @Test
  void lead_func_lead_fn_14()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_15")
  @Test
  void lead_func_lead_fn_15()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_16")
  @Test
  void lead_func_lead_fn_16()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_17")
  @Test
  void lead_func_lead_fn_17()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_18")
  @Test
  void lead_func_lead_fn_18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lead_func/lead_Fn_19")
  @Test
  void lead_func_lead_fn_19()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_20")
  @Test
  void lead_func_lead_fn_20()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lead_func/lead_Fn_21")
  @Test
  void lead_func_lead_fn_21()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lead_func/lead_Fn_22")
  @Test
  void lead_func_lead_fn_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_23")
  @Test
  void lead_func_lead_fn_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_24")
  @Test
  void lead_func_lead_fn_24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_25")
  @Test
  void lead_func_lead_fn_25()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_64")
  @Test
  void lead_func_lead_fn_64()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_65")
  @Test
  void lead_func_lead_fn_65()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_66")
  @Test
  void lead_func_lead_fn_66()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_67")
  @Test
  void lead_func_lead_fn_67()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lead_func/lead_Fn_68")
  @Test
  void lead_func_lead_fn_68()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_71")
  @Test
  void lead_func_lead_fn_71()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.UNSUPPORTED_NULL_ORDERING)
  @DrillTest("lead_func/lead_Fn_72")
  @Test
  void lead_func_lead_fn_72()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("aggregates/testW_Nulls_1")
  @Test
  void aggregates_test_w_nulls_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("first_val/firstValFn_18")
  @Test
  void first_val_first_val_fn_18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("first_val/firstValFn_19")
  @Test
  void first_val_first_val_fn_19()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("first_val/firstValFn_21")
  @Test
  void first_val_first_val_fn_21()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("first_val/firstValFn_22")
  @Test
  void first_val_first_val_fn_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("first_val/firstValFn_24")
  @Test
  void first_val_first_val_fn_24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("first_val/firstValFn_25")
  @Test
  void first_val_first_val_fn_25()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_17")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_17()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_20")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_20()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_26")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_26()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_27")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_27()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_28")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_28()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_30")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_30()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_47")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_47()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_48")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_48()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_49")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_49()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_100")
  @Test
  void lag_func_lag_fn_100()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_102")
  @Test
  void lag_func_lag_fn_102()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_103")
  @Test
  void lag_func_lag_fn_103()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_41")
  @Test
  void lag_func_lag_fn_41()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_42")
  @Test
  void lag_func_lag_fn_42()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_45")
  @Test
  void lag_func_lag_fn_45()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_63")
  @Test
  void lag_func_lag_fn_63()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_96")
  @Test
  void lag_func_lag_fn_96()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_97")
  @Test
  void lag_func_lag_fn_97()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lag_func/lag_Fn_99")
  @Test
  void lag_func_lag_fn_99()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_18")
  @Test
  void last_val_last_val_fn_18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_19")
  @Test
  void last_val_last_val_fn_19()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_21")
  @Test
  void last_val_last_val_fn_21()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_22")
  @Test
  void last_val_last_val_fn_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_24")
  @Test
  void last_val_last_val_fn_24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_25")
  @Test
  void last_val_last_val_fn_25()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("last_val/lastValFn_33")
  @Test
  void last_val_last_val_fn_33()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_36")
  @Test
  void lead_func_lead_fn_36()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_45")
  @Test
  void lead_func_lead_fn_45()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_91")
  @Test
  void lead_func_lead_fn_91()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_92")
  @Test
  void lead_func_lead_fn_92()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_94")
  @Test
  void lead_func_lead_fn_94()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_95")
  @Test
  void lead_func_lead_fn_95()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_97")
  @Test
  void lead_func_lead_fn_97()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_COUNT_MISMATCH)
  @DrillTest("lead_func/lead_Fn_98")
  @Test
  void lead_func_lead_fn_98()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_10")
  @Test
  void aggregates_agg_own_fn_10()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_12")
  @Test
  void aggregates_agg_own_fn_12()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_14")
  @Test
  void aggregates_agg_own_fn_14()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_15")
  @Test
  void aggregates_agg_own_fn_15()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_17")
  @Test
  void aggregates_agg_own_fn_17()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_18")
  @Test
  void aggregates_agg_own_fn_18()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_19")
  @Test
  void aggregates_agg_own_fn_19()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_22")
  @Test
  void aggregates_agg_own_fn_22()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_23")
  @Test
  void aggregates_agg_own_fn_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_39")
  @Test
  void aggregates_agg_own_fn_39()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/aggOWnFn_40")
  @Test
  void aggregates_agg_own_fn_40()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_50")
  @Test
  void aggregates_agg_own_fn_50()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/winFnQry_10")
  @Test
  void aggregates_win_fn_qry_10()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_16")
  @Test
  void aggregates_win_fn_qry_16()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/winFnQry_18")
  @Test
  void aggregates_win_fn_qry_18()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_1")
  @Test
  void aggregates_win_fn_qry_1()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_26")
  @Test
  void aggregates_win_fn_qry_26()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_28")
  @Test
  void aggregates_win_fn_qry_28()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_2")
  @Test
  void aggregates_win_fn_qry_2()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_31")
  @Test
  void aggregates_win_fn_qry_31()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_3")
  @Test
  void aggregates_win_fn_qry_3()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_46")
  @Test
  void aggregates_win_fn_qry_46()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_47")
  @Test
  void aggregates_win_fn_qry_47()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_48")
  @Test
  void aggregates_win_fn_qry_48()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_49")
  @Test
  void aggregates_win_fn_qry_49()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_4")
  @Test
  void aggregates_win_fn_qry_4()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_50")
  @Test
  void aggregates_win_fn_qry_50()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_51")
  @Test
  void aggregates_win_fn_qry_51()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_52")
  @Test
  void aggregates_win_fn_qry_52()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_53")
  @Test
  void aggregates_win_fn_qry_53()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_54")
  @Test
  void aggregates_win_fn_qry_54()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_55")
  @Test
  void aggregates_win_fn_qry_55()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_5")
  @Test
  void aggregates_win_fn_qry_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/winFnQry_84")
  @Test
  void aggregates_win_fn_qry_84()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("aggregates/winFnQry_85")
  @Test
  void aggregates_win_fn_qry_85()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_8")
  @Test
  void aggregates_win_fn_qry_8()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_17")
  @Test
  void aggregates_wo_ordr_by_17()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_18")
  @Test
  void aggregates_wo_ordr_by_18()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_19")
  @Test
  void aggregates_wo_ordr_by_19()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_20")
  @Test
  void aggregates_wo_ordr_by_20()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_21")
  @Test
  void aggregates_wo_ordr_by_21()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_22")
  @Test
  void aggregates_wo_ordr_by_22()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_23")
  @Test
  void aggregates_wo_ordr_by_23()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_24")
  @Test
  void aggregates_wo_ordr_by_24()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_25")
  @Test
  void aggregates_wo_ordr_by_25()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_26")
  @Test
  void aggregates_wo_ordr_by_26()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_10")
  @Test
  void aggregates_wo_prtn_by_10()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_12")
  @Test
  void aggregates_wo_prtn_by_12()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_13")
  @Test
  void aggregates_wo_prtn_by_13()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_14")
  @Test
  void aggregates_wo_prtn_by_14()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_15")
  @Test
  void aggregates_wo_prtn_by_15()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_17")
  @Test
  void aggregates_wo_prtn_by_17()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_18")
  @Test
  void aggregates_wo_prtn_by_18()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_19")
  @Test
  void aggregates_wo_prtn_by_19()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_20")
  @Test
  void aggregates_wo_prtn_by_20()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_25")
  @Test
  void aggregates_wo_prtn_by_25()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_3")
  @Test
  void aggregates_wo_prtn_by_3()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_4")
  @Test
  void aggregates_wo_prtn_by_4()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_5")
  @Test
  void aggregates_wo_prtn_by_5()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_6")
  @Test
  void aggregates_wo_prtn_by_6()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_7")
  @Test
  void aggregates_wo_prtn_by_7()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_8")
  @Test
  void aggregates_wo_prtn_by_8()
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_9")
  @Test
  void aggregates_wo_prtn_by_9()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_17")
  @Test
  void first_val_first_val_fn_17()
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_23")
  @Test
  void first_val_first_val_fn_23()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_1")
  @Test
  void frameclause_default_frame_rbupacr_bgint_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_2")
  @Test
  void frameclause_default_frame_rbupacr_bgint_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_3")
  @Test
  void frameclause_default_frame_rbupacr_bgint_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_4")
  @Test
  void frameclause_default_frame_rbupacr_bgint_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_6")
  @Test
  void frameclause_default_frame_rbupacr_bgint_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_7")
  @Test
  void frameclause_default_frame_rbupacr_bgint_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_3")
  @Test
  void frameclause_default_frame_rbupacr_chr_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_5")
  @Test
  void frameclause_default_frame_rbupacr_chr_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_1")
  @Test
  void frameclause_default_frame_rbupacr_dbl_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_2")
  @Test
  void frameclause_default_frame_rbupacr_dbl_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_3")
  @Test
  void frameclause_default_frame_rbupacr_dbl_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_6")
  @Test
  void frameclause_default_frame_rbupacr_dbl_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_7")
  @Test
  void frameclause_default_frame_rbupacr_dbl_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_1")
  @Test
  void frameclause_default_frame_rbupacr_dt_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_2")
  @Test
  void frameclause_default_frame_rbupacr_dt_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_4")
  @Test
  void frameclause_default_frame_rbupacr_dt_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_5")
  @Test
  void frameclause_default_frame_rbupacr_dt_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int10")
  @Test
  void frameclause_default_frame_rbupacr_int10()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int13")
  @Test
  void frameclause_default_frame_rbupacr_int13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_int14")
  @Test
  void frameclause_default_frame_rbupacr_int14()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int8")
  @Test
  void frameclause_default_frame_rbupacr_int8()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int9")
  @Test
  void frameclause_default_frame_rbupacr_int9()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_3")
  @Test
  void frameclause_default_frame_rbupacr_vchr_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_5")
  @Test
  void frameclause_default_frame_rbupacr_vchr_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/avg_mulwds")
  @Test
  void frameclause_multipl_wnwds_avg_mulwds()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/count_mulwds")
  @Test
  void frameclause_multipl_wnwds_count_mulwds()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/multipl_wnwds/fval_mulwds")
  @Test
  void frameclause_multipl_wnwds_fval_mulwds()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/multipl_wnwds/lval_mulwds")
  @Test
  void frameclause_multipl_wnwds_lval_mulwds()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/multipl_wnwds/mulwind_08")
  @Test
  void frameclause_multipl_wnwds_mulwind_08()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/multipl_wnwds/mulwind_09")
  @Test
  void frameclause_multipl_wnwds_mulwind_09()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/sum_mulwds")
  @Test
  void frameclause_multipl_wnwds_sum_mulwds()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_6")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_7")
  @Test
  void frameclause_rbcracr_rbcracr_bgint_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_char_3")
  @Test
  void frameclause_rbcracr_rbcracr_char_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_char_4")
  @Test
  void frameclause_rbcracr_rbcracr_char_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_char_5")
  @Test
  void frameclause_rbcracr_rbcracr_char_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_6")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_7")
  @Test
  void frameclause_rbcracr_rbcracr_dbl_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_1")
  @Test
  void frameclause_rbcracr_rbcracr_dt_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_2")
  @Test
  void frameclause_rbcracr_rbcracr_dt_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_4")
  @Test
  void frameclause_rbcracr_rbcracr_dt_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_5")
  @Test
  void frameclause_rbcracr_rbcracr_dt_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_int_13")
  @Test
  void frameclause_rbcracr_rbcracr_int_13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_int_14")
  @Test
  void frameclause_rbcracr_rbcracr_int_14()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_int_6")
  @Test
  void frameclause_rbcracr_rbcracr_int_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_int_7")
  @Test
  void frameclause_rbcracr_rbcracr_int_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_3")
  @Test
  void frameclause_rbcracr_rbcracr_vchar_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_4")
  @Test
  void frameclause_rbcracr_rbcracr_vchar_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_5")
  @Test
  void frameclause_rbcracr_rbcracr_vchar_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_1")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_2")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_3")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_4")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_6")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_7")
  @Test
  void frameclause_rbupacr_rbupacr_bgint_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_3")
  @Test
  void frameclause_rbupacr_rbupacr_chr_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_5")
  @Test
  void frameclause_rbupacr_rbupacr_chr_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_1")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_2")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_3")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_6")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_7")
  @Test
  void frameclause_rbupacr_rbupacr_dbl_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int10")
  @Test
  void frameclause_rbupacr_rbupacr_int10()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int13")
  @Test
  void frameclause_rbupacr_rbupacr_int13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBUPACR/RBUPACR_int14")
  @Test
  void frameclause_rbupacr_rbupacr_int14()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_3")
  @Test
  void frameclause_rbupacr_rbupacr_vchr_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_5")
  @Test
  void frameclause_rbupacr_rbupacr_vchr_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_4")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_6")
  @Test
  void frameclause_rbupauf_rbupauf_bgint_6()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_3")
  @Test
  void frameclause_rbupauf_rbupauf_char_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_6")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_6()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_7")
  @Test
  void frameclause_rbupauf_rbupauf_dbl_7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_1")
  @Test
  void frameclause_rbupauf_rbupauf_dt_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_2")
  @Test
  void frameclause_rbupauf_rbupauf_dt_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_4")
  @Test
  void frameclause_rbupauf_rbupauf_dt_4()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_5")
  @Test
  void frameclause_rbupauf_rbupauf_dt_5()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_13")
  @Test
  void frameclause_rbupauf_rbupauf_int_13()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_3")
  @Test
  void frameclause_rbupauf_rbupauf_vchar_3()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_53")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_53()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_54")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_54()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_55")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_55()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_57")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_57()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_58")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_58()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_59")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_59()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_60")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_60()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_61")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_61()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_62")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_62()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_63")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_63()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("frameclause/subQueries/frmInSubQry_64")
  @Test
  void frameclause_sub_queries_frm_in_sub_qry_64()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_101")
  @Test
  void lag_func_lag_fn_101()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_6")
  @Test
  void lag_func_lag_fn_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_10")
  @Test
  void last_val_last_val_fn_10()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_11")
  @Test
  void last_val_last_val_fn_11()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_12")
  @Test
  void last_val_last_val_fn_12()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_13")
  @Test
  void last_val_last_val_fn_13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_14")
  @Test
  void last_val_last_val_fn_14()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_15")
  @Test
  void last_val_last_val_fn_15()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_16")
  @Test
  void last_val_last_val_fn_16()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_17")
  @Test
  void last_val_last_val_fn_17()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_1")
  @Test
  void last_val_last_val_fn_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_20")
  @Test
  void last_val_last_val_fn_20()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_23")
  @Test
  void last_val_last_val_fn_23()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_2")
  @Test
  void last_val_last_val_fn_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_32")
  @Test
  void last_val_last_val_fn_32()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_36")
  @Test
  void last_val_last_val_fn_36()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_3")
  @Test
  void last_val_last_val_fn_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_4")
  @Test
  void last_val_last_val_fn_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_6")
  @Test
  void last_val_last_val_fn_6()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_7")
  @Test
  void last_val_last_val_fn_7()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_8")
  @Test
  void last_val_last_val_fn_8()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("last_val/lastValFn_9")
  @Test
  void last_val_last_val_fn_9()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_33")
  @Test
  void lead_func_lead_fn_33()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_42")
  @Test
  void lead_func_lead_fn_42()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_51")
  @Test
  void lead_func_lead_fn_51()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_54")
  @Test
  void lead_func_lead_fn_54()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_60")
  @Test
  void lead_func_lead_fn_60()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_63")
  @Test
  void lead_func_lead_fn_63()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_6")
  @Test
  void lead_func_lead_fn_6()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_77")
  @Test
  void lead_func_lead_fn_77()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_90")
  @Test
  void lead_func_lead_fn_90()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_96")
  @Test
  void lead_func_lead_fn_96()
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_9")
  @Test
  void lead_func_lead_fn_9()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/basic_3")
  @Test
  void nested_aggs_basic_3()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_5")
  @Test
  void nested_aggs_basic_5()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_7")
  @Test
  void nested_aggs_basic_7()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/cte_win_05")
  @Test
  void nested_aggs_cte_win_05()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_13")
  @Test
  void nested_aggs_emty_ovr_cls_13()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/emtyOvrCls_8")
  @Test
  void nested_aggs_emty_ovr_cls_8()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg24")
  @Test
  void nested_aggs_nstdagg24()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/woutPrtnBy_1")
  @Test
  void nested_aggs_wout_prtn_by_1()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/woutPrtnBy_2")
  @Test
  void nested_aggs_wout_prtn_by_2()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/woutPrtnBy_3")
  @Test
  void nested_aggs_wout_prtn_by_3()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/woutPrtnBy_4")
  @Test
  void nested_aggs_wout_prtn_by_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("nestedAggs/woutPrtnBy_5")
  @Test
  void nested_aggs_wout_prtn_by_5()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_15")
  @Test
  void ntile_func_ntile_fn_15()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_17")
  @Test
  void ntile_func_ntile_fn_17()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_27")
  @Test
  void ntile_func_ntile_fn_27()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_45")
  @Test
  void ntile_func_ntile_fn_45()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.RESULT_MISMATCH)
  @DrillTest("ntile_func/ntileFn_59")
  @Test
  void ntile_func_ntile_fn_59()
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_7")
  @Test
  void ntile_func_ntile_fn_7()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm01")
  @Test
  void frameclause_multipl_wnwds_rnk_no_frm01()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm02")
  @Test
  void frameclause_multipl_wnwds_rnk_no_frm02()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm03")
  @Test
  void frameclause_multipl_wnwds_rnk_no_frm03()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm04")
  @Test
  void frameclause_multipl_wnwds_rnk_no_frm04()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm05")
  @Test
  void frameclause_multipl_wnwds_rnk_no_frm05()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm06")
  @Test
  void frameclause_multipl_wnwds_rnk_no_frm06()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_1")
  @Test
  void frameclause_rbupacr_rbupacr_dt_1()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_2")
  @Test
  void frameclause_rbupacr_rbupacr_dt_2()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_4")
  @Test
  void frameclause_rbupacr_rbupacr_dt_4()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_5")
  @Test
  void frameclause_rbupacr_rbupacr_dt_5()
  {
    windowQueryTest();
  }

  @NotYetSupported(Modes.T_ALLTYPES_ISSUES)
  @DrillTest("frameclause/RBUPACR/RBUPACR_int7")
  @Test
  void frameclause_rbupacr_rbupacr_int7()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int8")
  @Test
  void frameclause_rbupacr_rbupacr_int8()
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int9")
  @Test
  void frameclause_rbupacr_rbupacr_int9()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_33")
  @Test
  void lag_func_lag_fn_33()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_51")
  @Test
  void lag_func_lag_fn_51()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_60")
  @Test
  void lag_func_lag_fn_60()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_77")
  @Test
  void lag_func_lag_fn_77()
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_95")
  @Test
  void lag_func_lag_fn_95()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause03")
  @Test
  void nested_aggs_frmclause03()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause12")
  @Test
  void nested_aggs_frmclause12()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause16")
  @Test
  void nested_aggs_frmclause16()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_6")
  @Test
  void nested_aggs_multi_win_6()
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_8")
  @Test
  void nested_aggs_multi_win_8()
  {
    windowQueryTest();
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
