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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import org.apache.druid.initialization.TombstoneDataStorageModule;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@JsonTypeName(TombstoneDataStorageModule.SCHEME)
public class TombstoneLoadSpec implements LoadSpec
{
  @Override
  public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
  {
    try {
      return new LoadSpecResult(writeFactoryFile(destDir));
    }
    catch (IOException e) {
      throw new SegmentLoadingException(
          "Failed to create factory.json for tombstone in dir [%s]",
          destDir.getAbsolutePath()
      );

    }
  }

  @VisibleForTesting
  public static int writeFactoryFile(File destDir) throws IOException
  {
    final String factoryJSONString = "{\"type\":\"tombstoneSegmentFactory\"}";
    final File factoryJson = new File(destDir, "factory.json");
    factoryJson.createNewFile();
    Files.write(factoryJSONString.getBytes(StandardCharsets.UTF_8), factoryJson);
    return factoryJSONString.length();
  }
}
