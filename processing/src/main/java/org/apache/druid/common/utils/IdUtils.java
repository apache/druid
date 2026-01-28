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

package org.apache.druid.common.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdUtils
{
  private static final Pattern INVALIDCHARS = Pattern.compile("(?s).*[^\\S ].*");

  private static final Joiner UNDERSCORE_JOINER = Joiner.on("_");

  public static String validateId(String thingToValidate, String stringToValidate)
  {
    if (Strings.isNullOrEmpty(stringToValidate)) {
      throw InvalidInput.exception("Invalid value for field [%s]: must not be null", thingToValidate);
    }
    if (stringToValidate.startsWith(".")) {
      throw InvalidInput.exception(
          "Invalid value for field [%s]: Value [%s] cannot start with '.'.",
          thingToValidate,
          stringToValidate
      );
    }
    if (stringToValidate.contains("/")) {
      throw InvalidInput.exception(
          "Invalid value for field [%s]: Value [%s] cannot contain '/'.",
          thingToValidate,
          stringToValidate
      );
    }

    Matcher m = INVALIDCHARS.matcher(stringToValidate);
    if (m.matches()) {
      throw InvalidInput.exception(
          "Invalid value for field [%s]: Value [%s] contains illegal whitespace characters.  Only space is allowed.",
          thingToValidate,
          stringToValidate
      );
    }

    for (int i = 0; i < stringToValidate.length(); i++) {
      final char c = stringToValidate.charAt(i);

      // Curator doesn't permit any of the following ranges, so we can't either, because IDs are often used as
      // znode paths. The first two ranges are control characters, the second two ranges correspond to surrogate
      // pairs. This means that characters outside the basic multilingual plane, such as emojis, are not allowed. 😢
      if (c > 0 && c < 31 || c > 127 && c < 159 || c > '\ud800' && c < '\uf8ff' || c > '\ufff0' && c < '\uffff') {
        throw InvalidInput.exception(
            "Invalid value for field [%s]: Value [%s] contains illegal UTF8 character [#%d] at position [%d]",
            thingToValidate,
            stringToValidate,
            (int) c,
            i
        );
      }
    }

    return stringToValidate;
  }

  public static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((ThreadLocalRandom.current().nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }

  public static String getRandomIdWithPrefix(String prefix)
  {
    return UNDERSCORE_JOINER.join(prefix, IdUtils.getRandomId());
  }

  public static String newTaskId(String typeName, String dataSource, @Nullable Interval interval)
  {
    return newTaskId(null, typeName, dataSource, interval);
  }

  public static String newTaskId(
      @Nullable String idPrefix,
      String typeName,
      String dataSource,
      @Nullable Interval interval
  )
  {
    return newTaskId(idPrefix, getRandomId(), DateTimes.nowUtc(), typeName, dataSource, interval);
  }

  /**
   * This method is only visible to outside only for testing.
   * Use {@link #newTaskId(String, String, Interval)} or {@link #newTaskId(String, String, String, Interval)} instead.
   */
  @VisibleForTesting
  static String newTaskId(
      @Nullable String idPrefix,
      String idSuffix,
      DateTime now,
      String typeName,
      String dataSource,
      @Nullable Interval interval
  )
  {
    final List<String> objects = new ArrayList<>();
    if (idPrefix != null) {
      objects.add(idPrefix);
    }
    objects.add(typeName);
    objects.add(dataSource);
    objects.add(idSuffix);
    if (interval != null) {
      objects.add(interval.getStart().toString());
      objects.add(interval.getEnd().toString());
    }
    objects.add(now.toString());

    return String.join("_", objects);
  }

  /**
   * Tries to parse the serialized ID as a {@link SegmentId} of the given datasource.
   *
   * @throws DruidException if the segment ID could not be parsed.
   */
  public static SegmentId getValidSegmentId(String dataSource, String serializedSegmentId)
  {
    final SegmentId parsedSegmentId = SegmentId.tryParse(dataSource, serializedSegmentId);
    if (parsedSegmentId == null) {
      throw InvalidInput.exception(
          "Could not parse segment ID[%s] for datasource[%s]",
          serializedSegmentId, dataSource
      );
    } else {
      return parsedSegmentId;
    }
  }

  /**
   * Tries to parse the given serialized IDs as {@link SegmentId}s of the given
   * datasource.
   *
   * @return Set containing valid segment IDs.
   * @throws DruidException if any of the given segment IDs is invalid
   */
  public static Set<SegmentId> getValidSegmentIds(String dataSource, Set<String> serializedIds)
  {
    final Set<SegmentId> validSegmentIds = new HashSet<>();
    final Set<String> invalidIds = new HashSet<>();

    for (String id : serializedIds) {
      final SegmentId validId = SegmentId.tryParse(dataSource, id);
      if (validId == null) {
        invalidIds.add(id);
      } else {
        validSegmentIds.add(validId);
      }
    }

    if (!invalidIds.isEmpty()) {
      throw InvalidInput.exception(
          "Could not parse segment IDs[%s] for datasource[%s]",
          invalidIds, dataSource
      );
    }

    return validSegmentIds;
  }
}
