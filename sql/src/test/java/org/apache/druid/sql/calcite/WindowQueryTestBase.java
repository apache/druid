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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.Injector;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.commons.io.FileUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.TimestampParser;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.planner.PlannerCaptureHook;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@SqlTestFrameworkConfig.ComponentSupplier(WindowQueryTestBase.DrillComponentSupplier.class)
public abstract class WindowQueryTestBase extends BaseCalciteQueryTest
{
  static {
    NullHandling.initializeForTests();
  }

  @RegisterExtension
  private final DisableUnless.DisableUnlessRule disableWhenNonSqlCompat = DisableUnless.SQL_COMPATIBLE;

  @RegisterExtension
  private final NotYetSupported.NotYetSupportedProcessor ignoreProcessor = new NotYetSupported.NotYetSupportedProcessor();

  @RegisterExtension
  protected TestCaseLoaderRule testCaseLoaderRule;

  protected static class WindowTestCase
  {
    protected final String query;
    protected final List<String[]> results;
    protected String filename;
    protected String resourcePath;

    protected WindowTestCase(String filename, String resourcePath)
    {
      try {
        this.filename = filename;
        this.resourcePath = resourcePath;
        this.query = readStringFromResource(".q");
        String resultsStr = readStringFromResource(".e");
        String[] lines = resultsStr.split("\n");
        results = new ArrayList<>();
        if (!resultsStr.isEmpty()) {
          for (String string : lines) {
            String[] cols = string.split("\t");
            results.add(cols);
          }
        }
      }
      catch (Exception e) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Encountered exception while loading testcase [%s]", filename),
            e
        );
      }
    }

    @Nonnull
    protected String getQueryString()
    {
      return query;
    }

    @Nonnull
    protected List<String[]> getExpectedResults()
    {
      return results;
    }

    @Nonnull
    protected String readStringFromResource(String s) throws IOException
    {
      final String query;
      try (InputStream queryIn = ClassLoader.getSystemResourceAsStream(resourcePath + filename + s)) {
        query = new String(ByteStreams.toByteArray(queryIn), StandardCharsets.UTF_8);
      }
      return query;
    }
  }

  protected abstract static class TestCaseLoaderRule implements BeforeEachCallback
  {
    protected WindowTestCase testCase = null;

    @Override
    public void beforeEach(ExtensionContext context)
    {
      Method method = context.getTestMethod().get();
      testCase = loadTestCase(method);
    }

    protected abstract WindowTestCase loadTestCase(Method method);
  }

  protected static class DrillComponentSupplier extends SqlTestFramework.StandardComponentSupplier
  {
    public DrillComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory,
        Injector injector
    )
    {
      final SpecificSegmentsQuerySegmentWalker retVal = super.createQuerySegmentWalker(
          conglomerate,
          joinableFactory,
          injector);

      final File tmpFolder = tempDirProducer.newTempFolder();
      TestDataBuilder.attachIndexesForDrillTestDatasources(retVal, tmpFolder);
      return retVal;
    }
  }

  private class TextualResultsVerifier implements ResultsVerifier
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
        Assert.assertEquals(expectedResultRowSignature, rowSignature);
      }
      currentRowSignature = rowSignature;
    }

    @Override
    public void verify(String sql, QueryTestRunner.QueryResults queryResults)
    {
      List<Object[]> results = queryResults.results;
      List<Object[]> expectedResults = parseResults(currentRowSignature, expectedResultsText);
      try {
        Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResultsText.size(), results.size());
        if (!isOrdered(queryResults)) {
          // in case the resultset is not ordered; order via the same comparator before comparison
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

    private boolean isOrdered(QueryTestRunner.QueryResults queryResults)
    {
      SqlNode sqlNode = queryResults.capture.getSqlNode();
      return SqlToRelConverter.isOrdered(sqlNode);
    }
  }

  private static class ArrayRowCmp implements Comparator<Object[]>
  {
    @Override
    public int compare(Object[] arg0, Object[] arg1)
    {
      String s0 = Arrays.toString(arg0);
      String s1 = Arrays.toString(arg1);
      return s0.compareTo(s1);
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
    if (val.isEmpty()) {
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

  protected void windowQueryTest()
  {
    WindowTestCase testCase = getCurrentTestCase();
    Thread thread = null;
    String oldName = null;
    try {
      thread = Thread.currentThread();
      oldName = thread.getName();
      thread.setName("windowQuery-" + testCase.filename);

      testBuilder()
          .skipVectorize(true)
          .queryContext(ImmutableMap.of(
                            PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
                            PlannerCaptureHook.NEED_CAPTURE_HOOK, true,
                            QueryContexts.ENABLE_DEBUG, true
                        )
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

  protected abstract WindowTestCase getCurrentTestCase();

  protected void ensureAllDeclared(String resourcePath, Class<? extends WindowQueryTestBase> testClass, Class<? extends Annotation> annotationClass) throws Exception
  {
    final URL windowQueriesUrl = ClassLoader.getSystemResource(resourcePath);
    Path windowFolder = new File(windowQueriesUrl.toURI()).toPath();

    Set<String> allCases = FileUtils
        .streamFiles(windowFolder.toFile(), true, "q")
        .map(file -> windowFolder.relativize(file.toPath()).toString())
        .sorted()
        .collect(Collectors.toSet());

    for (Method method : testClass.getDeclaredMethods()) {
      Annotation ann = method.getAnnotation(annotationClass);
      if (method.getAnnotation(Test.class) == null || ann == null) {
        continue;
      }
      String value = (String) annotationClass.getMethod("value").invoke(ann);
      if (allCases.remove(value + ".q")) {
        continue;
      }
      fail(String.format(Locale.ENGLISH, "Testcase [%s] references invalid file [%s].", method.getName(), value));
    }

    for (String string : allCases) {
      string = string.substring(0, string.lastIndexOf('.'));
      System.out.printf(Locale.ENGLISH, "@%s(\"%s\")\n"
                                        + "@Test\n"
                                        + "public void test_%s() {\n"
                                        + "    windowQueryTest();\n"
                                        + "}\n",
                        annotationClass.getSimpleName(),
                        string,
                        string.replace('/', '_'));
    }
    assertEquals("Found some non-declared testcases; please add the new testcases printed to the console!", 0, allCases.size());
  }
}
