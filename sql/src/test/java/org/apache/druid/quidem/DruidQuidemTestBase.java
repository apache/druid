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

package org.apache.druid.quidem;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;
import net.hydromatic.quidem.Quidem.Config;
import net.hydromatic.quidem.Quidem.ConfigBuilder;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Util;
import org.apache.druid.concurrent.Threads;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.MultiComponentSupplier;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Execute Quidem tests in Druid.
 *
 * How these tests work:
 * <ol>
 * <li>Test cases are in .iq files - contract of these files is that they
 * produce themselves if it was executed without errors</li>
 * <li>Executor (this class) picks up these files and runs them as part of unit
 * testruns</li>
 * <li>System under test is connected via an adapter which looks like a JDBC
 * driver</li>
 * </ol>
 *
 * Example usage:
 * <ol>
 * <li>Write new .iq test as a under the appropriate directory; with command for
 * expectations but without specifying them.</li>
 * <li>Run the test - it will produce a ".iq.out" file next to the ".iq"
 * one.</li>
 * <li>Copy over the .iq.out to .iq to accept the changes</li>
 * </ol>
 *
 * To shorten the above 2 steps you can run the test with system property quidem.overwrite=true
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DruidQuidemTestBase
{
  public static final String IQ_SUFFIX = ".iq";
  /**
   * System property name for "overwrite mode"; note: empty value is treated as
   * true
   */
  private static final String OVERWRITE_PROPERTY = "quidem.overwrite";

  private static final String PROPERTY_FILTER = "quidem.filter";

  private final String filterStr;
  private final List<Pattern> filterPatterns;

  private DruidQuidemRunner druidQuidemRunner;

  public DruidQuidemTestBase(CommandHandler commandHandler)
  {
    this(new DruidQuidemRunner(commandHandler, Quidem::new));
  }

  public DruidQuidemTestBase(DruidQuidemRunner druidQuidemRunner)
  {
    this.filterStr = System.getProperty(PROPERTY_FILTER, null);
    this.filterPatterns = buildFilterPatterns(filterStr);
    this.druidQuidemRunner = druidQuidemRunner;
  }

  private List<Pattern> buildFilterPatterns(@Nullable String filterStr)
  {
    if (null == filterStr) {
      return Collections.emptyList();
    }

    final List<Pattern> filterPatterns = new ArrayList<>();
    for (String filterGlob : filterStr.split(",")) {
      if (!filterGlob.endsWith("*") && !filterGlob.endsWith(IQ_SUFFIX)) {
        filterGlob = filterStr + IQ_SUFFIX;
      }
      filterPatterns.add(
          Pattern.compile(
              Arrays.stream(filterGlob.split("\\*", -1))
                    .map(Pattern::quote)
                    .collect(Collectors.joining("[^/]*"))));
    }
    return filterPatterns;
  }

  protected static class QuidemTestCaseConfiguration
  {
    final String fileName;
    final String componentSupplierName;

    public QuidemTestCaseConfiguration(String componentSupplierName, String fileName)
    {
      this.fileName = fileName;
      this.componentSupplierName = componentSupplierName;
    }

    public String getTestName()
    {
      if (componentSupplierName == null) {
        return fileName;
      } else {
        return StringUtils.format("%s@%s", fileName, componentSupplierName);
      }
    }

    @Override
    public String toString()
    {
      return getTestName();
    }
  }

  public List<QuidemTestCaseConfiguration> getTestConfigs() throws IOException
  {
    List<QuidemTestCaseConfiguration> ret = new ArrayList<>();
    List<String> fileNames = getFileNames();
    for (String file : fileNames) {
      try {
        ret.addAll(getConfigurationsFor(file));
      }
      catch (Exception e) {
        throw DruidException.defensive(e, "While processing configurations for quidem file [%s]", file);
      }
    }
    return ret;
  }

  private List<QuidemTestCaseConfiguration> getConfigurationsFor(String testFileName) throws IOException
  {
    File inFile = new File(getTestRoot(), testFileName);
    List<QuidemTestCaseConfiguration> ret = new ArrayList<>();
    for (Class<? extends QueryComponentSupplier> supplier : collectSuppliers(inFile)) {
      String supplierName = supplier == null ? null : supplier.getSimpleName();
      ret.add(new QuidemTestCaseConfiguration(supplierName, testFileName));
    }
    return ret;
  }

  private List<Class<? extends QueryComponentSupplier>> collectSuppliers(File inFile) throws IOException
  {
    Set<Class<MultiComponentSupplier>> metaSuppliers = collectMetaSuppliers(inFile);

    switch (metaSuppliers.size()) {
      case 0:
        return Collections.singletonList(null);
      case 1:
        return MultiComponentSupplier.getSuppliers(Iterables.getOnlyElement(metaSuppliers));
      default:
        throw DruidException.defensive("Multiple MetaComponentSuppliers found [%s].", metaSuppliers);
    }
  }

  private Set<Class<MultiComponentSupplier>> collectMetaSuppliers(File inFile) throws IOException
  {
    Set<Class<MultiComponentSupplier>> metaSuppliers = new HashSet<>();

    List<String> lines = Files.readLines(inFile, StandardCharsets.UTF_8);
    for (String line : lines) {
      if (line.startsWith("!use")) {
        String[] parts = line.split(" ");
        if (parts.length == 2) {
          SqlTestFrameworkConfig cfg = SqlTestFrameworkConfig.fromURL(parts[1]);
          validateFrameworkConfig(cfg);
          if (MultiComponentSupplier.class.isAssignableFrom(cfg.componentSupplier)) {
            metaSuppliers.add((Class<MultiComponentSupplier>) cfg.componentSupplier);
          }
        }
      }
    }
    int nCommands = Iterables.size(Iterables.filter(lines, line -> line.startsWith("!")));
    if (!commandLimitIgnoredFiles.contains(inFile.getName()) && nCommands > 80) {
      throw DruidException.defensive(
          "There are too many commands [%s] in file [%s] which would make working with the test harder. "
          + "Please reduce the number of queries/commands/etc",
          nCommands,
          inFile
      );
    }
    return metaSuppliers;
  }

  @SuppressWarnings("unused")
  protected void validateFrameworkConfig(SqlTestFrameworkConfig cfg)
  {
    // no-op
  }

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  public void test(QuidemTestCaseConfiguration testConfig) throws Exception
  {
    final String testName = testConfig.getTestName();

    File inFile = new File(getTestRoot(), testConfig.fileName);
    final File outFile = new File(getTestRoot(), testName + ".out");
    try (AutoCloseable closeable = Threads.withThreadName(testName)) {
      druidQuidemRunner.run(inFile, outFile, testConfig.componentSupplierName);
    }
    catch (Error e) {
      // This catch is needed to workaround the way Quidem currently handles AssumptionViolatedException
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof AssumptionViolatedException) {
        AssumptionViolatedException assumptionViolatedException = (AssumptionViolatedException) cause;
        throw assumptionViolatedException;
      }
      throw e;
    }

  }

  public static class DruidQuidemRunner
  {
    private CommandHandler commandHandler;
    private Function<Config, Quidem> quidemMaker;

    public DruidQuidemRunner()
    {
      this(new DruidQuidemCommandHandler(), Quidem::new);
    }

    public DruidQuidemRunner(CommandHandler commandHandler, Function<Config, Quidem> quidemMaker)
    {
      this.commandHandler = commandHandler;
      this.quidemMaker = quidemMaker;
    }

    public void run(File inFile) throws Exception
    {
      File outFile = new File(inFile.getParent(), inFile.getName() + ".out");
      run(inFile, outFile, null);
    }

    public void run(File inFile, final File outFile, String componentSupplier) throws Exception
    {
      FileUtils.mkdirp(outFile.getParentFile());
      try (Reader reader = Util.reader(inFile);
           Writer writer = Util.printWriter(outFile);
           Closer closer = new Closer()) {

        DruidQuidemConnectionFactory connectionFactory = new DruidQuidemConnectionFactory();
        if (componentSupplier != null) {
          connectionFactory.onSet("componentSupplier", componentSupplier);
        }
        ConfigBuilder configBuilder = Quidem.configBuilder()
                                            .withConnectionFactory(connectionFactory)
                                            .withPropertyHandler(connectionFactory)
                                            .withEnv(connectionFactory::envLookup)
                                            .withCommandHandler(commandHandler);

        Config config = configBuilder
            .withReader(reader)
            .withWriter(writer)
            .withStackLimit(-1)
            .build();

        quidemMaker.apply(config).execute();
      }
      catch (Exception e) {
        throw new RE(e, "Encountered exception while running [%s]", inFile);
      }

      final String diff = DiffTestCase.diff(inFile, outFile);

      if (!diff.isEmpty()) {
        if (isOverwrite()) {
          Files.copy(outFile, inFile);
        } else if (isUnsupportedComponentSupplier(diff, componentSupplier)) {
          System.out.println("Skipping verification of unsupported componentSupplier " + componentSupplier);
        } else {
          fail("Files differ: " + outFile + " " + inFile + "\n" + diff);
        }
      } else {
        if (outFile.exists()) {
          outFile.delete();
        }
      }
    }


    public static boolean isOverwrite()
    {
      String property = System.getProperty(OVERWRITE_PROPERTY, "false");
      return property.length() == 0 || Boolean.valueOf(property);
    }

    private static boolean isUnsupportedComponentSupplier(String diff, String componentSupplier)
    {
      return diff.contains(StringUtils.format(
          "Unsupported componentSupplier[%s], skipping verification of diff.",
          componentSupplier
      ));
    }
  }

  protected CommandHandler getCommandHandler()
  {
    return new DruidQuidemCommandHandler();
  }

  protected final List<String> getFileNames() throws IOException
  {
    List<String> ret = new ArrayList<>();

    File testRoot = getTestRoot();
    if (!testRoot.exists()) {
      throw new FileNotFoundException(StringUtils.format("testRoot [%s] doesn't exists!", testRoot));
    }

    for (File f : Files.fileTraverser().breadthFirst(testRoot)) {
      if (isTestIncluded(testRoot, f)) {
        Path relativePath = testRoot.toPath().relativize(f.toPath());
        ret.add(relativePath.toString());
      }
    }
    if (ret.isEmpty()) {
      throw new IAE(
          "There are no test cases in directory[%s] or there are no matches to filter[%s]",
          testRoot,
          filterStr
      );
    }
    Collections.sort(ret);
    return ret;
  }

  private boolean isTestIncluded(File testRoot, File f)
  {
    String relativePath = testRoot.toPath().relativize(f.toPath()).toString();
    return !f.isDirectory()
           && f.getName().endsWith(IQ_SUFFIX)
           && (filterPatterns.isEmpty() || filterPatterns.stream()
                                                         .anyMatch(pattern -> pattern.matcher(relativePath).matches()));
  }

  protected abstract File getTestRoot();

  @AfterAll
  public static void afterAll()
  {
    DruidAvaticaTestDriver.CONFIG_STORE.close();
  }

  /**
   * This list should eventually be removed; as soon as all tests become smaller than 80.
   */
  Set<String> commandLimitIgnoredFiles = ImmutableSet.of(
      "qaSQL_scalar_string.iq",
      "qaSQL_scalar_numeric.iq",
      "qaAggFuncs_array_agg_timestamp.iq",
      "qaAggFuncs_array_agg_double.iq",
      "qaAggFuncs_array_agg_float.iq",
      "qaAggFuncs_array_agg_long.iq",
      "qaAggFuncs_array_agg_string.iq",
      "qaWin_sql.iq",
      "qaAggFuncs_string_agg_timestamp.iq",
      "qaAggFuncs_string_agg_double.iq",
      "qaAggFuncs_string_agg_float.iq",
      "qaAggFuncs_string_agg_long.iq",
      "qaAggFuncs_string_agg_string.iq",
      "qaUnnest_array_sql_scalar_funcs.iq",
      "qaUnnest_mv_sql_scalar_funcs.iq",
      "qaUnnest_mv_sql_other_funcs.iq",
      "qaUnnest_mv_sql.iq",
      "qaWin_orderby_range_negative_first_last.iq",
      "qaWin_orderby_range_negative_sum_count.iq",
      "qaWin_orderby_rows_negative_first_last.iq",
      "qaWin_orderby_rows_negative_sum_count.iq",
      "qaWin_basics.iq",
      "qaWin_orderby_range_0_following_first_last.iq",
      "qaWin_orderby_range_0_following_sum_count.iq",
      "qaWin_orderby_range_0_preceding_first_last.iq",
      "qaWin_orderby_range_0_preceding_sum_count.iq",
      "qaWin_orderby_range_1_following_first_last.iq",
      "qaWin_orderby_range_1_following_sum_count.iq",
      "qaWin_orderby_range_1_preceding_first_last.iq",
      "qaWin_orderby_range_1_preceding_sum_count.iq",
      "qaWin_orderby_range_current_first_last.iq",
      "qaWin_orderby_range_current_sum_count.iq",
      "qaWin_orderby_range_ub_following_first_last.iq",
      "qaWin_orderby_range_ub_following_sum_count.iq",
      "qaWin_orderby_range_ub_preceding_first_last.iq",
      "qaWin_orderby_range_ub_preceding_sum_count.iq",
      "qaWin_orderby_rows_0_following_first_last.iq",
      "qaWin_orderby_rows_0_following_sum_count.iq",
      "qaWin_orderby_rows_0_preceding_first_last.iq",
      "qaWin_orderby_rows_0_preceding_sum_count.iq",
      "qaWin_orderby_rows_1_following_first_last.iq",
      "qaWin_orderby_rows_1_following_sum_count.iq",
      "qaWin_orderby_rows_1_preceding_first_last.iq",
      "qaWin_orderby_rows_1_preceding_sum_count.iq",
      "qaWin_orderby_rows_current_first_last.iq",
      "qaWin_orderby_rows_current_sum_count.iq",
      "qaWin_orderby_rows_ub_following_first_last.iq",
      "qaWin_orderby_rows_ub_following_sum_count.iq",
      "qaWin_orderby_rows_ub_preceding_first_last.iq",
      "qaWin_orderby_rows_ub_preceding_sum_count.iq",
      "qaUnnest_array_sql_other_funcs.iq",
      "qaJsonCols_funcs_and_sql.iq",
      "qaUnnest_array_sql.iq",
      "qaUnnest_array_sql_filter.iq",
      "qaUnnest_mv_sql_filter.iq",
      "qaArray_sql.iq",
      "qaArray_ops_funcs.iq"
  );
}
