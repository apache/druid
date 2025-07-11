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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.joda.time.Interval;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for the raw Map-based payload of a {@code Task}.
 * <p>
 * The builder does not use any defaults and all required fields must be set
 * explicitly.
 *
 * @see #ofType(String) to create a builder
 */
public class TaskPayload
{
  private final String type;

  private String dataSource;
  private Map<String, Object> inputSource = null;
  private Map<String, Object> inputFormat = null;
  private Map<String, Object> partitionsSpec = null;
  private Map<String, Object> granularitySpec = null;
  private Map<String, Object> timestampSpec = null;
  private Map<String, Object> dimensionsSpec = null;
  private Map<String, Object> splitHintSpec = null;

  private final Map<String, Object> context = new HashMap<>();
  private final List<Map<Object, Object>> metricsSpec = new ArrayList<>();

  private Integer maxNumConcurrentSubTasks = null;
  private Boolean forceGuaranteedRollup = null;
  private Long awaitSegmentAvailabilityTimeoutMillis = null;
  private Boolean appendToExisting = null;

  private TaskPayload(String type)
  {
    this.type = type;
  }

  /**
   * Initializes builder for a new {@code Task} for the given datasource.
   */
  public static TaskPayload ofType(String type)
  {
    return new TaskPayload(type);
  }

  public TaskPayload dataSource(String dataSource)
  {
    this.dataSource = dataSource;
    return this;
  }

  /**
   * Creates a raw Map-based payload for a {@code Task} that may be submitted to
   * the Overlord using {@code OverlordClient.runTask()}.
   */
  public Object withId(String taskId)
  {
    Preconditions.checkNotNull(taskId, "Task ID must not be null");
    Preconditions.checkNotNull(type, "Task type must be specified");
    Preconditions.checkNotNull(inputSource, "'inputSource' must be specified");
    Preconditions.checkNotNull(dataSource, "'dataSource' must be specified");

    return mapOf(
        "id", taskId,
        "type", type,
        "context", context,
        "spec",
        mapOf(
            "ioConfig",
            mapOf(
                "type", type,
                "inputSource", inputSource,
                "inputFormat", inputFormat,
                "appendToExisting", appendToExisting
            ),
            "tuningConfig",
            mapOf(
                "type", type,
                "partitionsSpec", partitionsSpec,
                "maxNumConcurrentSubTasks", maxNumConcurrentSubTasks,
                "forceGuaranteedRollup", forceGuaranteedRollup,
                "splitHintSpec", splitHintSpec,
                "awaitSegmentAvailabilityTimeoutMillis", awaitSegmentAvailabilityTimeoutMillis
            ),
            "dataSchema",
            mapOf(
                "dataSource", dataSource,
                "timestampSpec", timestampSpec,
                "dimensionsSpec", dimensionsSpec,
                "metricsSpec", metricsSpec.isEmpty() ? null : metricsSpec,
                "granularitySpec", granularitySpec
            )
        )
    );
  }

  public TaskPayload inputSource(Map<String, Object> jsonMap)
  {
    this.inputSource = jsonMap;
    return this;
  }

  public TaskPayload inlineInputSourceWithData(String data)
  {
    return inputSource(Map.of("type", "inline", "data", data));
  }

  public TaskPayload druidInputSource(String dataSource, Interval interval)
  {
    return inputSource(Map.of("type", "druid", "interval", interval, "dataSource", dataSource));
  }

  /**
   * Gets the absolute path of the given resource files and sets:
   * <pre>
   * "inputSource": {
   *   "type": "local",
   *   "files": [&lt;absolute-paths-of-given-resource-files&gt;]
   * }
   * </pre>
   */
  public TaskPayload localInputSourceWithFiles(String... files)
  {
    try {
      final List<String> filePaths = new ArrayList<>();
      for (String file : files) {
        final URL resourceUrl = getClass().getClassLoader().getResource(file);
        if (resourceUrl == null) {
          throw new ISE("Could not find file[%s]", file);
        }

        filePaths.add(new File(resourceUrl.toURI()).getAbsolutePath());
      }

      return inputSource(Map.of("type", "local", "files", filePaths));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public TaskPayload inputFormat(Map<String, Object> jsonMap)
  {
    this.inputFormat = jsonMap;
    return this;
  }

  public TaskPayload jsonInputFormat()
  {
    this.inputFormat = Map.of("type", "json");
    return this;
  }

  public TaskPayload csvInputFormatWithColumns(String... columns)
  {
    return inputFormat(
        Map.of("type", "csv", "findColumnsFromHeader", "false", "columns", List.of(columns))
    );
  }

  public TaskPayload appendToExisting(boolean append)
  {
    this.appendToExisting = append;
    return this;
  }

  public TaskPayload partitionsSpec(Map<String, Object> jsonMap)
  {
    this.partitionsSpec = jsonMap;
    return this;
  }

  public TaskPayload dynamicPartitionWithMaxRows(int maxRowsPerSegment)
  {
    this.partitionsSpec = Map.of("type", "dynamic", "maxRowsPerSegment", maxRowsPerSegment);
    return this;
  }

  public TaskPayload granularitySpec(Map<String, Object> jsonMap)
  {
    this.granularitySpec = jsonMap;
    return this;
  }

  public TaskPayload granularitySpec(String segmentGranularity, String queryGranularity, boolean rollup)
  {
    this.granularitySpec = Map.of(
        "segmentGranularity", segmentGranularity,
        "queryGranularity", queryGranularity,
        "rollup", rollup
    );
    return this;
  }

  /**
   * Sets {@code "granularitySpec": {"segmentGranularity": <arg>}}.
   */
  public TaskPayload segmentGranularity(String granularity)
  {
    return granularitySpec(Map.of("segmentGranularity", granularity));
  }

  public TaskPayload timestampSpec(Map<String, Object> jsonMap)
  {
    this.timestampSpec = jsonMap;
    return this;
  }

  public TaskPayload isoTimestampColumn(String timestampColumn)
  {
    return timestampSpec(Map.of("format", "iso", "column", timestampColumn));
  }

  public TaskPayload timestampColumn(String timestampColumn)
  {
    return timestampSpec(Map.of("column", timestampColumn));
  }

  public TaskPayload dimensionsSpec(Map<String, Object> jsonMap)
  {
    this.dimensionsSpec = jsonMap;
    return this;
  }

  /**
   * Sets {@code "dimensionSpec": {"dimensions": [<arg>]}}.
   */
  public TaskPayload dimensions(String... dimensions)
  {
    return dimensionsSpec(Map.of("dimensions", List.of(dimensions)));
  }

  public TaskPayload metricAggregate(String name, String type)
  {
    return metricAggregate(name, type, name);
  }

  public TaskPayload metricAggregate(String name, String type, String fieldName)
  {
    this.metricsSpec.add(mapOf("type", type, "name", name, "fieldName", fieldName));
    return this;
  }

  public TaskPayload metricAggregate(Map<String, Object> jsonMap)
  {
    this.metricsSpec.add(Map.copyOf(jsonMap));
    return this;
  }

  public TaskPayload maxConcurrentSubTasks(int maxNumConcurrentSubTasks)
  {
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    return this;
  }

  public TaskPayload forceGuaranteedRollup(boolean rollup)
  {
    this.forceGuaranteedRollup = rollup;
    return this;
  }

  public TaskPayload splitHintSpec(Map<String, Object> jsonMap)
  {
    this.splitHintSpec = jsonMap;
    return this;
  }

  public TaskPayload awaitSegmentAvailabilityTimeoutMillis(long millis)
  {
    this.awaitSegmentAvailabilityTimeoutMillis = millis;
    return this;
  }

  public TaskPayload context(String key, Object value)
  {
    this.context.put(key, value);
    return this;
  }

  /**
   * Creates a map using only the non-null key-value pairs.
   *
   * @param kvPairs key1, value1, key2, value2, ...
   * @return null if none of the key-value pairs are non-null.
   */
  private static Map<Object, Object> mapOf(Object... kvPairs)
  {
    if (kvPairs.length % 2 > 0) {
      throw new ISE("Key and value must be given in pairs.");
    }

    final Map<Object, Object> map = new HashMap<>();
    for (int i = 0; i < kvPairs.length - 1; i += 2) {
      if (kvPairs[i] != null && kvPairs[i + 1] != null) {
        map.put(kvPairs[i], kvPairs[i + 1]);
      }
    }

    return map.isEmpty() ? null : Map.copyOf(map);
  }
}
