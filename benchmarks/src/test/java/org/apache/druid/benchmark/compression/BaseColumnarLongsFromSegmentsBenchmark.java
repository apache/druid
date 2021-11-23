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

package org.apache.druid.benchmark.compression;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;

@State(Scope.Benchmark)
public class BaseColumnarLongsFromSegmentsBenchmark extends BaseColumnarLongsBenchmark
{
  /**
   * Long columns to read from the segment file specified by {@link #segmentPath}
   */
  @Param({
      "__time",
      "followers",
      "friends",
      "max_followers",
      "max_retweets",
      "max_statuses",
      "retweets",
      "statuses",
      "tweets"
  })
  String columnName;

  /**
   * Number of rows in the segment. This should actually match the number of rows specified in {@link #segmentPath}. If
   * it is smaller than only this many rows will be read, if larger then the benchmark will explode trying to read more
   * data than exists rows.
   *
   * This is a hassle, but ensures that the row count ends up in the output measurements.
   */
  @Param({"3259585"})
  int rows;


  /**
   * Path to a segment file to read long columns from. This shouldn't really be used as a parameter, but is nice to
   * be included in the output measurements.
   *
   * This is BYO segment, as this file doesn't probably exist for you, replace it and other parameters with the segment
   * to test.
   */
  @Param({"tmp/segments/twitter-ticker-1/"})
  String segmentPath;

  /**
   * Friendly name of the segment. Like {@link #segmentPath}, this shouldn't really be used as a parameter, but is also
   * nice to be included in the output measurements.
   */
  @Param({"twitter-ticker"})
  String segmentName;

  String getColumnEncodedFileName(String encoding, String segmentName, String columnName)
  {
    return StringUtils.format("%s-%s-longs-%s.bin", encoding, segmentName, columnName);
  }

  File getTmpDir() throws IOException
  {
    final String dirPath = StringUtils.format("tmp/encoding/%s", segmentName);
    File dir = new File(dirPath);
    FileUtils.mkdirp(dir);
    return dir;
  }
}
