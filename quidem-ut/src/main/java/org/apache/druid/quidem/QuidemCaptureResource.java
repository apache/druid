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

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.hook.DruidHookDispatcher;

import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.io.File;
import java.net.URI;

@Path("/quidem")
@LazySingleton
public class QuidemCaptureResource
{
  public static final File RECORD_PATH = ProjectPathUtils
      .getPathFromProjectRoot("quidem-ut/src/test/quidem/org.apache.druid.quidem.QTest");
  private URI quidemURI;
  private QuidemRecorder recorder = null;
  private DruidHookDispatcher hookDispatcher;

  @Inject
  public QuidemCaptureResource(@Named("quidem") URI quidemURI, DruidHookDispatcher hookDispatcher)
  {
    this.quidemURI = quidemURI;
    this.hookDispatcher = hookDispatcher;
  }

  @GET
  @Path("/start")
  @Produces(MediaType.TEXT_PLAIN)
  public synchronized String start()
  {
    stopIfRunning();
    recorder = new QuidemRecorder(
        quidemURI,
        hookDispatcher,
        genRecordFilePath()
    );
    return recorder.toString();
  }

  private File genRecordFilePath()
  {
    String fileName = StringUtils.format("record-%d.iq", System.currentTimeMillis());
    return new File(RECORD_PATH, fileName);
  }

  private synchronized void stopIfRunning()
  {
    if (recorder != null) {
      recorder.close();
      recorder = null;
    }

  }
}
