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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.LongsColumn;
import org.apache.druid.segment.column.ValueType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Set;

@State(Scope.Benchmark)
public class BaseColumnarLongsFromSegmentsBenchmark extends BaseColumnarLongsBenchmark
{
  //CHECKSTYLE.OFF: Regexp
  // twitter-ticker
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

  @Param({"3259585"})
  int rows;


  @Param({"tmp/segments/twitter-ticker-1/"})
  String segmentPath;

  @Param({"twitter-ticker"})
  String segmentName;


  //CHECKSTYLE.ON: Regexp

  private static IndexIO INDEX_IO;
  public static ObjectMapper JSON_MAPPER;

  void initializeValues() throws IOException
  {
    initializeSegmentValueIntermediaryFile();
    File dir = getTmpDir();
    File dataFile = new File(dir, getColumnDataFileName(segmentName, columnName));

    ArrayList<Long> values = Lists.newArrayList();
    try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        long value = Long.parseLong(line);
        if (value < minValue) {
          minValue = value;
        }
        if (value > maxValue) {
          maxValue = value;
        }
        values.add(value);
        rows++;
      }
    }

    vals = values.stream().mapToLong(i -> i).toArray();
  }


  String getColumnDataFileName(String segmentName, String columnName)
  {
    return StringUtils.format("%s-longs-%s.txt", segmentName, columnName);
  }

  String getColumnEncodedFileName(String encoding, String segmentName, String columnName)
  {
    return StringUtils.format("%s-%s-longs-%s.bin", encoding, segmentName, columnName);
  }

  File getTmpDir()
  {
    final String dirPath = StringUtils.format("tmp/encoding/%s", segmentName);
    File dir = new File(dirPath);
    dir.mkdirs();
    return dir;
  }

  /**
   * writes column values to text file, 1 per line
   *
   * @throws IOException
   */
  void initializeSegmentValueIntermediaryFile() throws IOException
  {
    File dir = getTmpDir();
    File dataFile = new File(dir, getColumnDataFileName(segmentName, columnName));

    if (!dataFile.exists()) {
      JSON_MAPPER = new DefaultObjectMapper();
      INDEX_IO = new IndexIO(
          JSON_MAPPER,
          () -> 0
      );
      try (final QueryableIndex index = INDEX_IO.loadIndex(new File(segmentPath))) {
        final Set<String> columnNames = Sets.newLinkedHashSet();
        columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
        Iterables.addAll(columnNames, index.getColumnNames());
        final ColumnHolder column = index.getColumnHolder(columnName);
        final ColumnCapabilities capabilities = column.getCapabilities();
        final ValueType columnType = capabilities.getType();
        try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
          if (columnType != ValueType.LONG) {
            throw new RuntimeException("Invalid column type, expected 'Long'");
          }
          LongsColumn theColumn = (LongsColumn) column.getColumn();


          for (int i = 0; i < theColumn.length(); i++) {
            long value = theColumn.getLongSingleValueRow(i);
            writer.write(value + "\n");
          }
        }
      }
    }
  }
}
