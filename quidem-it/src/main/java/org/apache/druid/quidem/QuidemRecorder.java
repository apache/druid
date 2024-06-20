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

import org.apache.druid.sql.calcite.run.DruidHook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class QuidemRecorder implements AutoCloseable, DruidHook<String>
{
  private PrintStream printStream;
  private File file;

  public QuidemRecorder(URI quidemURI, File file)
  {
    this.file = file;
    try {
      this.printStream = new PrintStream(new FileOutputStream(file), true, StandardCharsets.UTF_8.name());
    }
    catch (UnsupportedEncodingException | FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    printStream.println("#started " + new Date());
    printStream.println("!use " + quidemURI);
    printStream.println("!set outputformat mysql");
    DruidHook.register(DruidHook.SQL, this);
  }

  @Override
  public void close()
  {
    DruidHook.unregister(DruidHook.SQL, this);
  }

  @Override
  public void invoke(HookKey<String> key, String query)
  {
    if (DruidHook.SQL.equals(key)) {
      printStream.print(query);
      printStream.println(";");
      printStream.println("!ok");
      return;
    }
  }

  @Override
  public String toString()
  {
    return "QuidemRecorder [file=" + file + "]";
  }

}
