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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StreamUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.hyperic.jni.ArchLoaderException;
import org.hyperic.jni.ArchNotSupportedException;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarLoader;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class SigarUtil
{
  private static final Logger log = new Logger(SigarUtil.class);

  // Note: this is required to load the sigar native lib.
  static {
    SigarLoader loader = new SigarLoader(Sigar.class);
    try {
      String libName = loader.getLibraryName();

      final URL url = SysMonitor.class.getResource("/" + libName);
      if (url != null) {
        final File tmpDir = FileUtils.createTempDir("sigar");
        // As per java.io.DeleteOnExitHook.runHooks() deletion order is reversed from registration order
        tmpDir.deleteOnExit();
        final File nativeLibTmpFile = new File(tmpDir, libName);
        nativeLibTmpFile.deleteOnExit();
        StreamUtils.copyToFileAndClose(url.openStream(), nativeLibTmpFile);
        log.info("Loading sigar native lib at tmpPath[%s]", nativeLibTmpFile);
        loader.load(nativeLibTmpFile.getParent());
      } else {
        log.info("No native libs found in jar, letting the normal load mechanisms figger it out.");
      }
    }
    catch (ArchNotSupportedException | ArchLoaderException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Sigar getSigar()
  {
    return new Sigar();
  }

  /**
   * CurrentProcessIdHolder class is initialized after SigarUtil, that guarantees that new Sigar() is executed after
   * static block (which loads the library) of SigarUtil is executed. This is anyway guaranteed by JLS if the static
   * field goes below the static block in textual order, but fragile e. g. if someone applies automatic reformatting and
   * the static field is moved above the static block.
   */
  private static class CurrentProcessIdHolder
  {
    private static final long CURRENT_PROCESS_ID = new Sigar().getPid();
  }

  public static long getCurrentProcessId()
  {
    return CurrentProcessIdHolder.CURRENT_PROCESS_ID;
  }

}
