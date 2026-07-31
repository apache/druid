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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fallback {@link PartialLoadMatcher} used as the {@code defaultImpl} so that older Druid versions can still parse a
 * rule whose matcher type was introduced in a newer version. {@link #match} always returns {@code null} (matcher does
 * not apply); the rule then defers to its configured {@link CannotMatchBehavior}, so whether the segment falls through
 * the cascade or gets full-loaded on this tier is up to the rule author. A warning is logged once per instance
 * lifetime to surface the configuration mismatch without flooding the logs across coordinator passes.
 * <p>
 * Note: this matcher is not lossless to round-trip. An older coordinator that reads a rule with an unknown matcher
 * type and then re-serializes it will not preserve the original {@code type} discriminator or any matcher-specific
 * configuration. The expected operational pattern is to upgrade the coordinator to a version that recognizes the
 * matcher rather than rely on round-trip.
 */
public class UnknownPartialLoadMatcher implements PartialLoadMatcher
{
  private static final Logger log = new Logger(UnknownPartialLoadMatcher.class);

  private final AtomicBoolean warned = new AtomicBoolean(false);

  @Override
  @Nullable
  public MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec)
  {
    if (warned.compareAndSet(false, true)) {
      log.warn(
          "Encountered an unknown PartialLoadMatcher type in a partial load rule. The matcher will be treated as"
          + " not applicable; the rule's onCannotMatch behavior determines the outcome. Upgrade Druid to a version"
          + " that supports this matcher."
      );
    }
    return null;
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof UnknownPartialLoadMatcher;
  }

  @Override
  public int hashCode()
  {
    return UnknownPartialLoadMatcher.class.hashCode();
  }

  @Override
  public String toString()
  {
    return "UnknownPartialLoadMatcher{}";
  }
}
