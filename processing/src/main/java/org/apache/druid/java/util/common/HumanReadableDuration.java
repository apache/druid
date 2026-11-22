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

package org.apache.druid.java.util.common;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.DateTimeException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Non-negative, fixed-length duration with nanosecond resolution.
 * Bare values use the legacy unit, milliseconds by default.
 */
@JsonDeserialize(using = HumanReadableDurationDeserializer.class)
@JsonSerialize(using = HumanReadableDurationSerializer.class)
public final class HumanReadableDuration
{
  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final Pattern BARE_NUMBER = Pattern.compile("[0-9]+");
  private static final Pattern SUFFIXED_NUMBER = Pattern.compile("([0-9]+)(ns|us|ms|s|m|h|d)");

  public static final HumanReadableDuration ZERO = new HumanReadableDuration(0L);

  private final long nanos;

  private HumanReadableDuration(long nanos)
  {
    this.nanos = nanos;
  }

  public static HumanReadableDuration ofNanos(long nanos)
  {
    if (nanos < 0) {
      throw new IAE("Duration cannot be negative: %d ns", nanos);
    }
    return new HumanReadableDuration(nanos);
  }

  public static HumanReadableDuration ofMillis(long millis)
  {
    if (millis < 0) {
      throw new IAE("Duration cannot be negative: %d ms", millis);
    }
    try {
      return new HumanReadableDuration(Math.multiplyExact(millis, NANOS_PER_MILLI));
    }
    catch (ArithmeticException e) {
      throw new IAE(e, "Duration overflows nanosecond range: %d ms", millis);
    }
  }

  public static HumanReadableDuration parse(String value)
  {
    return parse(value, TimeUnit.MILLISECONDS);
  }

  /**
   * Parse a duration using {@code legacyUnit} only for bare, unsuffixed values.
   * Explicit suffixes and ISO-8601 values always have their specified units.
   */
  public static HumanReadableDuration parse(String value, TimeUnit legacyUnit)
  {
    if (value == null || value.trim().isEmpty()) {
      throw new IAE("Duration must not be null or blank");
    }
    if (legacyUnit == null) {
      throw new IAE("Legacy time unit must not be null");
    }

    final String text = value.trim();
    if (text.startsWith("P")) {
      try {
        final Duration duration = Duration.parse(text);
        if (duration.isNegative()) {
          throw new IAE("Duration cannot be negative: %s", text);
        }
        return ofNanos(duration.toNanos());
      }
      catch (DateTimeException | ArithmeticException e) {
        throw new IAE(e, "Invalid or out-of-range ISO duration: %s", text);
      }
    }

    final long multiplier;
    final String digits;
    if (BARE_NUMBER.matcher(text).matches()) {
      multiplier = TimeUnit.NANOSECONDS.convert(1, legacyUnit);
      digits = text;
    } else {
      final Matcher matcher = SUFFIXED_NUMBER.matcher(text);
      if (!matcher.matches()) {
        throw new IAE("Invalid duration format: %s", text);
      }
      digits = matcher.group(1);
      multiplier = nanosPerUnit(matcher.group(2));
    }

    try {
      return new HumanReadableDuration(Math.multiplyExact(Long.parseLong(digits), multiplier));
    }
    catch (NumberFormatException | ArithmeticException e) {
      throw new IAE(e, "Duration is out of range: %s", text);
    }
  }

  private static long nanosPerUnit(String unit)
  {
    switch (unit) {
      case "ns":
        return 1L;
      case "us":
        return 1_000L;
      case "ms":
        return NANOS_PER_MILLI;
      case "s":
        return 1_000_000_000L;
      case "m":
        return 60_000_000_000L;
      case "h":
        return 3_600_000_000_000L;
      case "d":
        return 86_400_000_000_000L;
      default:
        throw new IAE("Unsupported duration unit: %s", unit);
    }
  }

  public long getNanos()
  {
    return nanos;
  }

  /** Round positive sub-millisecond durations up to avoid treating them as zero. */
  public long getMillis()
  {
    return nanos / NANOS_PER_MILLI + (nanos % NANOS_PER_MILLI == 0 ? 0 : 1);
  }

  public Duration toDuration()
  {
    return Duration.ofNanos(nanos);
  }

  @Override
  public String toString()
  {
    if (nanos % NANOS_PER_MILLI == 0) {
      return Long.toString(nanos / NANOS_PER_MILLI);
    }
    if (nanos % 1_000L == 0) {
      return nanos / 1_000L + "us";
    }
    return nanos + "ns";
  }

  @Override
  public boolean equals(Object other)
  {
    return this == other || other instanceof HumanReadableDuration
           && nanos == ((HumanReadableDuration) other).nanos;
  }

  @Override
  public int hashCode()
  {
    return Long.hashCode(nanos);
  }
}
