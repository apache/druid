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

package org.apache.druid.sql.calcite.tester;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.tester.LinesSection.ResultsSection;
import org.apache.druid.sql.calcite.tester.TestSection.Section;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Records the actual results of running a planner test so that
 * the results can be compared with expected, or, on an error,
 * emitted to an "actuals" file.
 */
public class ActualResults
{
  /**
   * Gathers errors found when verifying a test case against actual
   * results. The prefix allows a section to declare itself, then
   * invoke a generic verifier that doesn't know about the specific
   * section.
   */
  public static class ErrorCollector
  {
    private final List<String> errors = new ArrayList<>();
    private String prefix;

    public void setSection(String section)
    {
      prefix = section;
    }

    public void add(String error)
    {
      if (prefix != null) {
        error = prefix + ": " + error;
      }
      errors.add(error);
    }

    public boolean ok()
    {
      return errors.isEmpty();
    }

    public List<String> errors()
    {
      return errors;
    }
  }

  /**
   * Equivalent of a {@link TestSection}, but for actual results.
   * Holds a specific, labeled kind of actual results and tracks
   * if those actuals match the expected results.
   */
  public abstract static class ActualResultsSection
  {
    boolean ok;

    public abstract void verify(ErrorCollector errors);
    public abstract void write(TestCaseWriter writer) throws IOException;
  }

  /**
   * Simple string results, such as for an exception.
   */
  public static class StringResults extends ActualResultsSection
  {
    final PatternSection expected;
    final String actual;

    public StringResults(PatternSection expected, String actual)
    {
      this.expected = expected;
      this.actual = actual;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        expected.write(writer);
      } else {
        writer.emitSection(expected.name(), actual);
      }
    }

    @Override
    public void verify(ErrorCollector errors)
    {
      ok = expected.verify(actual, errors);
    }
  }

  /**
   * Results represented as a string array, such as when breaking a
   * block of text into lines, for matching line-by-line.
   */
  public static class StringArrayResults extends ActualResultsSection
  {
    final PatternSection expected;
    final String[] actual;

    public StringArrayResults(PatternSection expected, String[] actual)
    {
      this.expected = expected;
      this.actual = actual;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        expected.write(writer);
      } else {
        writer.emitSection(expected.name(), actual);
      }
    }

    @Override
    public void verify(ErrorCollector errors)
    {
      ok = expected.verify(actual, errors);
    }
  }

  /**
   * Query run results, when comparing as strings.
   */
  public static class RowResults extends ActualResultsSection
  {
    final ResultsSection expected;
    final List<String> actual;

    public RowResults(ResultsSection expected, List<String> actual)
    {
      this.expected = expected;
      this.actual = actual;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        expected.write(writer);
      } else {
        writer.emitSection(expected.name(), actual);
      }
    }

    @Override
    public void verify(ErrorCollector errors)
    {
      ok = expected.verify(actual, errors);
    }
  }

  /**
   * Actual query output when compared as Java objects. Handles the
   * case where a string compare is unstable (such as when results
   * contain float or double values.)
   */
  public static class JsonResults extends ActualResultsSection
  {
    final ResultsSection expected;
    final List<Object[]> actual;
    final ObjectMapper mapper;

    public JsonResults(
        ResultsSection expected,
        List<Object[]> actual,
        ObjectMapper mapper)
    {
      this.expected = expected;
      this.actual = actual;
      this.mapper = mapper;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        expected.write(writer);
      } else {
        List<String> lines = QueryTestCases.resultsToJson(actual, mapper);
        writer.emitSection(expected.name(), lines);
      }
    }

    @Override
    public void verify(ErrorCollector errors)
    {
      ok = expected.verify(actual, mapper, errors);
    }
  }

  /**
   * Results for an exception.
   *
   */
  public static class ExceptionResults extends ActualResultsSection
  {
    final TextSection.ExceptionSection expected;
    final Exception actual;

    public ExceptionResults(TextSection.ExceptionSection expected, Exception actual)
    {
      this.expected = expected;
      this.actual = actual;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        expected.write(writer);
      } else {
        writer.emitException(actual);
      }
    }

    @Override
    public void verify(ErrorCollector errors)
    {
      ok = expected.verify(actual, errors);
    }
  }

  /**
   * Actual resource action results.
   */
  public static class ResourceResults extends ActualResultsSection
  {
    final ResourcesSection expected;
    final Set<ResourceAction> actual;

    public ResourceResults(ResourcesSection expected, Set<ResourceAction> actual)
    {
      this.expected = expected;
      this.actual = actual;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        expected.write(writer);
      } else {
        writer.emitResources(actual);
      }
    }

    @Override
    public void verify(ErrorCollector errors)
    {
      ok = expected.verify(actual, errors);
    }
  }

  public static class ActualRun
  {
    final QueryRun run;
    final ActualResultsSection rows;
    final Exception actualException;
    final StringResults error;
    final ExceptionResults exception;
    final Map<String, Object> actualContext;
    private final boolean actualSqlCompatibleNulls;
    private boolean ok = true;

    public ActualRun(
        QueryRun run,
        Map<String, Object> actualContext,
        List<Object[]> rows,
        ObjectMapper mapper)
    {
      this.run = run;
      ResultsSection results = run.resultsSection();
      boolean typedCompare = run.booleanOption(OptionsSection.TYPED_COMPARE);
      if (typedCompare) {
        this.rows = new JsonResults(results, rows, mapper);
      } else {
        this.rows = new RowResults(results, QueryTestCases.resultsToJson(rows, mapper));
      }
      this.exception = null;
      this.error = null;
      this.actualException = null;
      this.actualContext = actualContext;
      this.actualSqlCompatibleNulls = NullHandling.sqlCompatible();
    }

    public ActualRun(QueryRun run, Map<String, Object> actualContext, Exception e)
    {
      this.run = run;
      this.rows = null;
      this.actualException = e;
      QueryTestCase testCase = run.testCase();
      TextSection.ExceptionSection exSection = testCase.exception();
      if (exSection == null) {
        this.exception = null;
      } else {
        this.exception = new ExceptionResults(exSection, e);
      }
      PatternSection errorSection = testCase.error();
      if (errorSection == null) {
        this.error = null;
      } else {
        this.error = new StringResults(errorSection, e.getMessage());
      }
      this.actualContext = actualContext;
      this.actualSqlCompatibleNulls = NullHandling.sqlCompatible();
    }

    public void verify(ErrorCollector errors)
    {
      errors.setSection(run.displayLabel());
      boolean shouldFail = run.shouldFail();
      if (shouldFail && actualException != null) {
        errors.add("Expected failure but run suceeded");
        ok = false;
        return;
      } else if (!shouldFail && actualException != null) {
        errors.add("Expected success but run failed");
        ok = false;
        return;
      }
      if (actualException != null) {
        if (exception != null) {
          exception.verify(errors);
          ok = exception.ok;
        }
        if (error != null) {
          error.verify(errors);
          ok &= error.ok;
        }
      } else if (rows != null) {
        rows.verify(errors);
        ok = rows.ok;
      }
    }

    public void write(TestCaseWriter writer) throws IOException
    {
      if (ok) {
        run.write(writer);
        return;
      }
      StringBuilder buf = new StringBuilder()
          .append("sqlCompatibleNulls=")
          .append(actualSqlCompatibleNulls)
          .append("\nContext:\n");
      for (Entry<String, Object> entry : actualContext.entrySet()) {
        buf.append(entry.getKey())
           .append("=")
           .append(entry.getValue())
           .append("\n");
      }
      writer.emitComment(buf.toString());
      writer.emitSection("run", run.label);

      if (actualException != null) {
        if (exception != null) {
          exception.write(writer);
        }
        if (error != null) {
          error.write(writer);
        }
        if (exception == null && error == null) {
          writer.emitException(actualException);
          writer.emitError(actualException);
        }
        return;
      }
      for (TestSection section : run.fileOrder) {
        if (section.section() == Section.RESULTS) {
          rows.write(writer);
        } else {
          section.write(writer);
        }
      }
    }
  }

  private final QueryTestCase testCase;
  protected StringResults ast;
  protected StringResults unparsed;
  protected StringResults plan;
  protected StringResults execPlan;
  protected ExceptionResults exception;
  protected StringResults error;
  protected StringResults explain;
  protected StringArrayResults schema;
  protected StringArrayResults targetSchema;
  protected StringResults nativeQuery;
  protected ResourceResults resourceActions;
  protected Exception actualException;
  protected List<ActualRun> runs = new ArrayList<>();
  private ActualResults.ErrorCollector errors = new ActualResults.ErrorCollector();

  public ActualResults(QueryTestCase testCase)
  {
    this.testCase = testCase;
  }

  public void exception(Exception e)
  {
    this.actualException = e;
    TextSection.ExceptionSection exSection = testCase.exception();
    if (exSection != null) {
      this.exception = new ExceptionResults(exSection, e);
    }
    PatternSection errorSection = testCase.error();
    if (errorSection != null) {
      this.error = new StringResults(errorSection, e.getMessage());
    }
  }

  public void unparsed(PatternSection section, String text)
  {
    this.unparsed = new StringResults(section, text);
  }

  public void ast(PatternSection section, String text)
  {
    this.ast = new StringResults(section, text);
  }

  public void plan(PatternSection section, String text)
  {
    this.plan = new StringResults(section, text);
  }

  public void execPlan(PatternSection section, String text)
  {
    this.execPlan = new StringResults(section, text);
  }

  public void schema(PatternSection section, String[] schema)
  {
    this.schema = new StringArrayResults(section, schema);
  }

  public void targetSchema(PatternSection section, String[] schema)
  {
    this.targetSchema = new StringArrayResults(section, schema);
  }

  public void nativeQuery(PatternSection section, String text)
  {
    this.nativeQuery = new StringResults(section, text);
  }

  public void resourceActions(ResourcesSection section, Set<ResourceAction> resourceActions)
  {
    this.resourceActions = new ResourceResults(section, resourceActions);
  }

  public void explain(PatternSection section, String text)
  {
    this.explain = new StringResults(section, text);
  }

  public void run(
      QueryRun run,
      Map<String, Object> actualContext,
      List<Object[]> rows,
      ObjectMapper mapper)
  {
    runs.add(new ActualRun(run, actualContext, rows, mapper));
  }

  public void runFailed(QueryRun run, Map<String, Object> actualContext, Exception e)
  {
    runs.add(new ActualRun(run, actualContext, e));
  }

  public ActualResults.ErrorCollector errors()
  {
    return errors;
  }

  public boolean ok()
  {
    return errors.ok();
  }

  public void verify()
  {
    verifyException();
    if (testCase.shouldFail() || !ok()) {
      return;
    }
    verify(ast);
    verify(unparsed);
    verify(plan);
    verify(execPlan);
    verify(schema);
    verify(targetSchema);
    verify(explain);
    verify(nativeQuery);
    verify(resourceActions);
    verifyRuns();
  }

  private void verify(ActualResultsSection section)
  {
    if (section != null) {
      section.verify(errors);
    }
  }

  public void verifyException()
  {
    boolean shouldFail = testCase.shouldFail();
    if (!shouldFail) {
      if (actualException != null) {
        errors.add(StringUtils.format(
            "Failed with exception %s: [%s]",
            actualException.getClass().getSimpleName(),
            actualException.getMessage()));
      }
      return;
    }
    if (shouldFail && actualException == null) {
      errors.add("Expected failure but got success");
      return;
    }
    verify(exception);
    verify(error);
  }

  public void verifyRuns()
  {
    for (ActualRun run : runs) {
      run.verify(errors);
    }
  }

  public void write(TestCaseWriter writer) throws IOException
  {
    writeSetup(writer);

    if (actualException == null) {
      writeResults(writer);
      writeRuns(writer);
    } else {
      writeFailure(writer);
    }
  }

  private void writeSetup(TestCaseWriter writer) throws IOException
  {
    for (TestSection section : testCase.sections()) {
      switch (section.section()) {
        case COMMENTS:
          section.write(writer);
          writer.emitErrors(errors.errors);
          break;
        case CASE:
        case SQL:
        case CONTEXT:
        case OPTIONS:
        case PARAMETERS:
          section.write(writer);
          break;
        default:
          break;
      }
    }
  }

  private void writeFailure(TestCaseWriter writer) throws IOException
  {
    if (testCase.shouldFail()) {
      for (TestSection section : testCase.sections()) {
        switch (section.section()) {
          case EXCEPTION:
            if (actualException == null) {
              section.write(writer);
            } else {
              exception.write(writer);
            }
            break;
          case ERROR:
            if (actualException == null) {
              section.write(writer);
            } else {
              error.write(writer);
            }
            break;
          default:
            break;
        }
      }
    } else {
      writer.emitException(actualException);
      writer.emitError(actualException);
    }
  }

  private void writeResults(TestCaseWriter writer) throws IOException
  {
    for (TestSection section : testCase.sections()) {
      switch (section.section()) {
        case AST:
          writeSection(section, ast, writer);
          break;
        case UNPARSED:
          writeSection(section, unparsed, writer);
          break;
        case EXPLAIN:
          writeSection(section, explain, writer);
          break;
        case PLAN:
          writeSection(section, plan, writer);
          break;
        case EXEC_PLAN:
          writeSection(section, execPlan, writer);
          break;
        case SCHEMA:
          writeSection(section, schema, writer);
          break;
        case TARGET_SCHEMA:
          writeSection(section, targetSchema, writer);
          break;
        case NATIVE:
          writeSection(section, nativeQuery, writer);
          break;
        case RESOURCES:
          writeSection(section, resourceActions, writer);
          break;
        case RESULTS:
          writeSection(section, null, writer);
          break;
        default:
          break;
      }
    }
  }

  private void writeRuns(TestCaseWriter writer) throws IOException
  {
    // Write the runs in order by the expected runs.
    boolean[] written = new boolean[runs.size()];
    for (QueryRun expectedRun : testCase.runs()) {
      boolean first = true;
      for (int i = 0; i < runs.size(); i++) {
        ActualRun actualRun = runs.get(i);
        if (actualRun.run == expectedRun) {
          if (first || !actualRun.ok) {
            actualRun.write(writer);
          }
          written[i] = true;
          first = false;
        }
      }
      if (first) {
        expectedRun.write(writer);
      }
    }
    for (int i = 0; i < runs.size(); i++) {
      if (!written[i]) {
        runs.get(i).write(writer);
      }
    }
  }

  private void writeSection(
      TestSection testSection,
      ActualResultsSection resultsSection,
      TestCaseWriter writer
  ) throws IOException
  {
    if (resultsSection != null) {
      resultsSection.write(writer);
    } else if (actualException == null) {
      testSection.write(writer);
    }
  }
}
