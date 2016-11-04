/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;


public class PortWriter
{
  private static final Logger log = new Logger(PortWriter.class);
  private final DruidNode node;
  private volatile File portFile;
  private volatile boolean started = false;

  @Inject
  public PortWriter(@Self DruidNode node)
  {
    this.node = node;
  }

  @LifecycleStart
  public synchronized void start()
  {
    if (started) {
      throw new ISE("Already started");
    }
    int port = node.getPort();
    final File portFile = new File(TierLocalTaskRunner.PORT_FILE_NAME);
    log.info("Writing port [%d] to [%s]", node.getPort(), portFile.getAbsoluteFile());
    if (portFile.exists()) {
      throw new ISE("Port file [%s] already exists! cannot start already started", portFile);
    }
    final String portString = Integer.toString(port);
    log.debug("Writing port [%d] to [%s]", port, portFile);
    final ByteBuffer buffer = ByteBuffer.wrap(StringUtils.toUtf8(portString));
    try (FileChannel portFileChannel = FileChannel.open(
        portFile.toPath(),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE
    )) {
      final FileLock fileLock = portFileChannel.lock();
      try {
        while (buffer.hasRemaining()) {
          portFileChannel.write(buffer);
        }
      }
      finally {
        fileLock.release();
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    this.portFile = portFile;
    started = true;
  }

  @LifecycleStop
  public synchronized void stop()
  {
    if (!started) {
      log.info("Already stopped, ignoring stop request");
    }
    log.info("Stopping");
    final File portFile = this.portFile;
    if (portFile != null) {
      log.debug("Erasing port");
      if (portFile.exists() && !portFile.delete()) {
        log.warn("Could not delete port file [%s]", portFile);
      }
    }
    started = false;
  }
}
