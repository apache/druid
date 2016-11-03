/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.hadoop.fs;

import com.metamx.common.logger.Logger;

import java.io.IOException;

/**
 * This wrapper class is created to be able to access some of the the "protected" methods inside Hadoop's
 * FileSystem class. Those are supposed to become public eventually or more appropriate alternatives would be
 * provided.
 * This is a hack and should be removed when no longer necessary.
 */
public class HadoopFsWrapper
{
  private static final Logger log = new Logger(HadoopFsWrapper.class);

  private HadoopFsWrapper() {}

  /**
   * Same as FileSystem.rename(from, to, Options.Rename.NONE) . That is,
   * it returns "false" when "to" directory already exists. It is different from FileSystem.rename(from, to)
   * which moves "from" directory inside "to" directory if it already exists.
   *
   * @param from
   * @param to
   * @return
   * @throws IOException
   */
  public static boolean rename(FileSystem fs, Path from, Path to) throws IOException
  {
    try {
      fs.rename(from, to, Options.Rename.NONE);
      return true;
    }
    catch (IOException ex) {
      log.warn(ex, "Failed to rename [%s] to [%s].", from, to);
      return false;
    }
  }
}
