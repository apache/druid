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

package org.apache.druid.segment;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods useful for implementing deep storage extensions.
 */
@PublicApi
public class SegmentUtils
{
  private static final HashFunction HASH_FUNCTION = Hashing.sha256();

  /**
   * Hash the IDs of the given segments based on SHA-256 algorithm.
   */
  public static String hashIds(List<DataSegment> segments)
  {
    Collections.sort(segments);
    final Hasher hasher = HASH_FUNCTION.newHasher();
    segments.forEach(segment -> hasher.putString(segment.getId().toString(), StandardCharsets.UTF_8));
    return StringUtils.fromUtf8(hasher.hash().asBytes());
  }

  public static int getVersionFromDir(File inDir) throws IOException
  {
    File versionFile = new File(inDir, "version.bin");
    if (versionFile.exists()) {
      return Ints.fromByteArray(Files.toByteArray(versionFile));
    }

    final File indexFile = new File(inDir, "index.drd");
    int version;
    if (indexFile.exists()) {
      try (InputStream in = new FileInputStream(indexFile)) {
        version = in.read();
      }
      return version;
    }

    throw new IOE("Invalid segment dir [%s]. Can't find either of version.bin or index.drd.", inDir);
  }

  private SegmentUtils()
  {
  }
}
