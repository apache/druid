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

import com.google.common.io.Files;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;
import net.hydromatic.quidem.Quidem.Config;
import net.hydromatic.quidem.Quidem.ConfigBuilder;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Util;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 * To shorten the above 2 steps
 *
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

  private FileFilter filter = TrueFileFilter.INSTANCE;

  private DruidQuidemRunner druidQuidemRunner;

  public DruidQuidemTestBase()
  {
    String filterStr = System.getProperty(PROPERTY_FILTER, null);
    if (filterStr != null) {
      if (!filterStr.endsWith("*") && !filterStr.endsWith(IQ_SUFFIX)) {
        filterStr = filterStr + IQ_SUFFIX;
      }
      filter = new WildcardFileFilter(filterStr);
    }
    druidQuidemRunner = new DruidQuidemRunner();
  }

  /** Creates a command handler. */
  protected CommandHandler createCommandHandler()
  {
    return Quidem.EMPTY_COMMAND_HANDLER;
  }

  @ParameterizedTest
  @MethodSource("getFileNames")
  public void test(String testFileName) throws Exception
  {
    File inFile = new File(getTestRoot(), testFileName);

    final File outFile = new File(inFile.getParentFile(), inFile.getName() + ".out");
    druidQuidemRunner.run(inFile, outFile);
  }

  public static class DruidQuidemRunner
  {
    public DruidQuidemRunner()
    {
    }

    public void run(File inFile) throws Exception
    {
      File outFile = new File(inFile.getParent(), inFile.getName() + ".out");
      run(inFile, outFile);
    }

    public void run(File inFile, final File outFile) throws Exception
    {
      FileUtils.mkdirp(outFile.getParentFile());
      try (Reader reader = Util.reader(inFile);
          Writer writer = Util.printWriter(outFile);
          Closer closer = new Closer()) {

        DruidQuidemConnectionFactory connectionFactory = new DruidQuidemConnectionFactory();
        ConfigBuilder configBuilder = Quidem.configBuilder()
            .withConnectionFactory(connectionFactory)
            // this is not nice - but it makes it possible to do queryContext
            // changes
            .withPropertyHandler(connectionFactory)
            .withCommandHandler(new DruidQuidemCommandHandler());

        Config config = configBuilder
            .withReader(reader)
            .withWriter(writer).build();

        new Quidem(config).execute();
      }
      catch (Exception e) {
        throw new RE(e, "Encountered exception while running [%s]", inFile);
      }

      final String diff = DiffTestCase.diff(inFile, outFile);

      if (!diff.isEmpty()) {
        if (isOverwrite()) {
          Files.copy(outFile, inFile);
        } else {
          fail("Files differ: " + outFile + " " + inFile + "\n" + diff);
        }
      }
    }

    public static boolean isOverwrite()
    {
      String property = System.getProperty(OVERWRITE_PROPERTY, "false");
      return property.length() == 0 || Boolean.valueOf(property);
    }
  }

  protected final List<String> getFileNames() throws IOException
  {
    List<String> ret = new ArrayList<String>();

    File testRoot = getTestRoot();
    if (!testRoot.exists()) {
      throw new FileNotFoundException(StringUtils.format("testRoot [%s] doesn't exists!", testRoot));
    }
    for (File f : testRoot.listFiles(this::isTestIncluded)) {
      ret.add(f.getName());
    }
    if (ret.isEmpty()) {
      throw new IAE(
          "There are no test cases in directory [%s] or there are no matches to filter [%s]",
          testRoot,
          filter
      );
    }
    Collections.sort(ret);
    return ret;
  }

  private boolean isTestIncluded(File f)
  {
    return !f.isDirectory()
        && f.getName().endsWith(IQ_SUFFIX)
        && filter.accept(f);
  }

  protected abstract File getTestRoot();

  @AfterAll
  public static void afterAll()
  {
    DruidAvaticaTestDriver.CONFIG_STORE.close();
  }
}
