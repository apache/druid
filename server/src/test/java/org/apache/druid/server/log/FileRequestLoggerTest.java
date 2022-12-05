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

package org.apache.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.RequestLogLine;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FileRequestLoggerTest
{
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static final String HOST = "localhost";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLog() throws Exception
  {
    ObjectMapper objectMapper = new ObjectMapper();
    DateTime dateTime = DateTimes.nowUtc();
    File logDir = temporaryFolder.newFolder();
    String nativeQueryLogString = dateTime + "\t" + HOST + "\t" + "native";
    String sqlQueryLogString = dateTime + "\t" + HOST + "\t" + "sql";

    FileRequestLogger fileRequestLogger = new FileRequestLogger(
        objectMapper,
        scheduler,
        logDir,
        "yyyy-MM-dd'.log'",
        null
    );
    fileRequestLogger.start();

    RequestLogLine nativeRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(nativeRequestLogLine.getNativeQueryLine(EasyMock.anyObject()))
            .andReturn(nativeQueryLogString)
            .anyTimes();
    RequestLogLine sqlRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(sqlRequestLogLine.getSqlQueryLine(EasyMock.anyObject())).andReturn(sqlQueryLogString).anyTimes();
    EasyMock.replay(nativeRequestLogLine, sqlRequestLogLine);

    fileRequestLogger.logNativeQuery(nativeRequestLogLine);
    fileRequestLogger.logSqlQuery(sqlRequestLogLine);

    File logFile = new File(logDir, dateTime.toString("yyyy-MM-dd'.log'"));
    String logString = CharStreams.toString(Files.newBufferedReader(logFile.toPath(), StandardCharsets.UTF_8));
    Assert.assertTrue(logString.contains(nativeQueryLogString + "\n" + sqlQueryLogString + "\n"));
    fileRequestLogger.stop();
  }

  @Test
  public void testLogRemove() throws Exception
  {
    ObjectMapper objectMapper = new ObjectMapper();
    File logDir = temporaryFolder.newFolder();
    DateTime dateTime = DateTimes.nowUtc();
    String logString = dateTime + "\t" + HOST + "\t" + "logString";

    File oldLogFile = new File(logDir, "2000-01-01.log");
    com.google.common.io.Files.write("testOldLogContent", oldLogFile, StandardCharsets.UTF_8);
    oldLogFile.setLastModified(new Date(0).getTime());
    FileRequestLogger fileRequestLogger = new FileRequestLogger(
        objectMapper,
        scheduler,
        logDir,
        "yyyy-MM-dd'.log'",
        Duration.standardDays(1)
    );
    fileRequestLogger.start();
    RequestLogLine nativeRequestLogLine = EasyMock.createMock(RequestLogLine.class);
    EasyMock.expect(nativeRequestLogLine.getNativeQueryLine(EasyMock.anyObject())).andReturn(logString).anyTimes();
    EasyMock.replay(nativeRequestLogLine);
    fileRequestLogger.logNativeQuery(nativeRequestLogLine);
    File logFile = new File(logDir, dateTime.toString("yyyy-MM-dd'.log'"));
    Thread.sleep(100);
    Assert.assertFalse(oldLogFile.exists());
    Assert.assertTrue(logFile.exists());
    fileRequestLogger.stop();
  }

  @Test
  public void testLogRemoveWithInvalidDuration() throws Exception
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("request logs retention period must be atleast P1D");
    ObjectMapper objectMapper = new ObjectMapper();
    File logDir = temporaryFolder.newFolder();
    FileRequestLogger fileRequestLogger = new FileRequestLogger(
        objectMapper,
        scheduler,
        logDir,
        "yyyy-MM-dd'.log'",
        Duration.standardHours(12)
    );
  }
}
