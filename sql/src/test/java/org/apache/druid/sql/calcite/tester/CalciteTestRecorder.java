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
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.util.CalciteTests;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Hooks into the various Calcite<foo>QueryTest classes to record
 * the tests. Generates a "starter" test case file from the recorded
 * values.
 * <p>
 * Note that JUnit runs the tests in the order that Java reports methods,
 * which is generally <b>not</b> the order in which they appear in the
 * source file. Use {@link TestCaseMerger} to reorder the tests to match
 * the source file order.
 */
public interface CalciteTestRecorder
{
  Logger log = new Logger(CalciteTestRecorder.class);

  class DummyRecorder implements CalciteTestRecorder
  {
    @Override
    public void record(CalciteTestCapture test)
    {
    }

    @Override
    public void emit()
    {
    }

    @Override
    public boolean isLive()
    {
      return false;
    }
  }

  class Recorder implements CalciteTestRecorder
  {
    private final boolean captureRun;
    private final List<CalciteTestCapture> tests = new ArrayList<>();
    private final ObjectMapper jsonMapper = CalciteTests.getJsonMapper();

    public Recorder(boolean captureRun)
    {
      this.captureRun = captureRun;
    }

    @Override
    public boolean isLive()
    {
      return true;
    }

    @Override
    public void record(CalciteTestCapture test)
    {
      if (test == null) {
        return;
      }
      if (tests.isEmpty()) {
        tests.add(test);
        return;
      }
      CalciteTestCapture prev = tests.get(tests.size() - 1);
      if (!prev.methodName.equals(test.methodName) ||
          !Objects.equals(prev.provider, test.provider)) {
        tests.add(test);
      }
    }

    @Override
    public void emit()
    {
      File dest = new File("target/actual/recorded.case");
      try {
        FileUtils.mkdirp(dest.getParentFile());
      }
      catch (IOException e) {
        throw new ISE("Cannot create directory: " + dest.getParent());
      }
      emit(dest);
    }

    private void emit(File dest)
    {
      try (Writer writer = new OutputStreamWriter(new FileOutputStream(dest), StandardCharsets.UTF_8)) {
        TestCaseWriter testWriter = new TestCaseWriter(writer);
        for (CalciteTestCapture test : tests) {
          test.write(captureRun, testWriter, jsonMapper);
        }
      }
      catch (IOException e) {
        log.warn(e, "Failed to emit recorded tests");
      }
    }
  }

  boolean isLive();
  void record(CalciteTestCapture test);
  void emit();

  enum Option
  {
    OFF,
    PLAN_ONLY,
    PLAN_AND_RUN
  }

  static CalciteTestRecorder create(Option option)
  {
    switch (option) {
      case PLAN_ONLY:
        return new Recorder(false);
      case PLAN_AND_RUN:
        return new Recorder(true);
      default:
        return new DummyRecorder();
    }
  }
}
