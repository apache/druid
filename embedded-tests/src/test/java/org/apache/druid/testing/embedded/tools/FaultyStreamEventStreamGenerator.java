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

package org.apache.druid.testing.embedded.tools;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link SyntheticStreamGenerator} that can inject faulty data (invalid format, null/empty fields,
 * multi-row data, etc.) into the stream at a configurable ratio. This is used to test the robustness
 * of streaming ingestion pipelines.
 *
 * <p>The generator wraps a delegate {@link EventSerializer} and produces three categories of events:
 * <ul>
 *   <li><b>Valid events</b> — normal Wikipedia-style events serialized by the delegate</li>
 *   <li><b>Faulty events</b> — events that may be invalid JSON, contain null/empty fields, or
 *       use multi-row format, depending on the configured {@link DataVariant}</li>
 * </ul>
 *
 * <p>The ratio of faulty events is controlled by {@link #faultyRatio} (0.0–1.0), where 0.0 means
 * all events are valid and 1.0 means all events are faulty.
 */
public class FaultyStreamEventStreamGenerator extends SyntheticStreamGenerator
{
  private static final Logger LOG = new Logger(FaultyStreamEventStreamGenerator.class);

  /**
   * The type of faulty data to inject into the stream.
   */
  public enum DataVariant
  {
    /** All events are valid (no faults injected). */
    ALL_VALID,
    /** Inject malformed JSON bytes that cannot be parsed. */
    INVALID_JSON,
    /** Inject valid JSON with null field values. */
    NULL_FIELDS,
    /** Inject empty JSON objects ({}). */
    EMPTY_JSON,
    /** Inject multi-row JSON arrays containing multiple objects. */
    MULTI_ROW,
    /** Inject completely empty strings. */
    EMPTY_STRING
  }

  private final EventSerializer delegate;
  private final DataVariant variant;
  private final double faultyRatio;

  /**
   * Creates a new faulty stream generator.
   *
   * @param delegate     the serializer used for valid events
   * @param eventsPerSecond number of events per second
   * @param cyclePaddingMs  padding for cycle timing
   * @param variant      the type of faulty data to inject
   * @param faultyRatio  the ratio (0.0–1.0) of faulty events to inject
   */
  public FaultyStreamEventStreamGenerator(
      final EventSerializer delegate,
      final int eventsPerSecond,
      final long cyclePaddingMs,
      final DataVariant variant,
      final double faultyRatio
  )
  {
    super(delegate, eventsPerSecond, cyclePaddingMs);
    this.delegate = delegate;
    this.variant = variant;
    this.faultyRatio = Math.max(0.0, Math.min(1.0, faultyRatio));
  }

  @Override
  List<Pair<String, Object>> newEvent(final int row, final DateTime timestamp)
  {
    // newEvent() is called by the parent's generateEvents() method, which bypasses
    // the delegate serializer and serializes the event itself. When the parent
    // serializes the event, it will use the delegate serializer, which will produce
    // valid JSON. Faulty data injection is handled by overriding the run() method
    // to replace some events with faulty bytes.
    final List<Pair<String, Object>> event = new ArrayList<>();
    event.add(Pair.of("timestamp", "2021-01-01T00:00:00Z"));
    event.add(Pair.of("page", "Test Page"));
    event.add(Pair.of("language", "en"));
    event.add(Pair.of("user", "test"));
    event.add(Pair.of("unpatrolled", "true"));
    event.add(Pair.of("newPage", "true"));
    event.add(Pair.of("robot", "false"));
    event.add(Pair.of("anonymous", "false"));
    event.add(Pair.of("namespace", "article"));
    event.add(Pair.of("continent", "North America"));
    event.add(Pair.of("country", "United States"));
    event.add(Pair.of("region", "Bay Area"));
    event.add(Pair.of("city", "San Francisco"));
    event.add(Pair.of("added", row));
    event.add(Pair.of("deleted", 0));
    event.add(Pair.of("delta", row));
    return Collections.unmodifiableList(event);
  }

  /**
   * Returns whether the event at the given index should be faulty based on the configured ratio.
   */
  public boolean isFaultyEvent(final int eventIndex)
  {
    if (variant == DataVariant.ALL_VALID) {
      return false;
    }
    return ThreadLocalRandom.current().nextDouble() < faultyRatio;
  }

  /**
   * Generates a faulty byte array for the given variant.
   */
  public byte[] generateFaultyBytes(final int eventIndex)
  {
    switch (variant) {
      case INVALID_JSON:
        return "{\"broken\": }".getBytes(StandardCharsets.UTF_8);
      case NULL_FIELDS:
        return ("{"
               + "\"timestamp\": null,"
               + "\"page\": null,"
               + "\"language\": null,"
               + "\"user\": null,"
               + "\"unpatrolled\": null,"
               + "\"newPage\": null,"
               + "\"robot\": null,"
               + "\"anonymous\": null,"
               + "\"namespace\": null,"
               + "\"continent\": null,"
               + "\"country\": null,"
               + "\"region\": null,"
               + "\"city\": null,"
               + "\"added\": null,"
               + "\"deleted\": null,"
               + "\"delta\": null"
               + "}")
               .getBytes(StandardCharsets.UTF_8);
      case EMPTY_JSON:
        return "{}".getBytes(StandardCharsets.UTF_8);
      case MULTI_ROW:
        return ("["
               + "{\"timestamp\":\"2021-01-01T00:00:00Z\",\"page\":\"Multi1\",\"language\":\"en\",\"user\":\"test\",\"unpatrolled\":\"true\",\"newPage\":\"true\",\"robot\":\"false\",\"anonymous\":\"false\",\"namespace\":\"article\",\"continent\":\"North America\",\"country\":\"United States\",\"region\":\"Bay Area\",\"city\":\"San Francisco\",\"added\":1,\"deleted\":0,\"delta\":1},"
               + "{\"timestamp\":\"2021-01-01T00:00:01Z\",\"page\":\"Multi2\",\"language\":\"en\",\"user\":\"test\",\"unpatrolled\":\"true\",\"newPage\":\"true\",\"robot\":\"false\",\"anonymous\":\"false\",\"namespace\":\"article\",\"continent\":\"North America\",\"country\":\"United States\",\"region\":\"Bay Area\",\"city\":\"San Francisco\",\"added\":2,\"deleted\":0,\"delta\":2}"
               + "\"]")

               .getBytes(StandardCharsets.UTF_8);
      case EMPTY_STRING:
        return "".getBytes(StandardCharsets.UTF_8);
      default:
        return "{}".getBytes(StandardCharsets.UTF_8);
    }
  }

  /**
   * Returns the variant of faulty data this generator is configured to produce.
   */
  public DataVariant getVariant()
  {
    return variant;
  }

  /**
   * Returns the ratio of faulty events.
   */
  public double getFaultyRatio()
  {
    return faultyRatio;
  }
}
