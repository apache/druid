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

package org.apache.druid.server.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.log.StartupLoggingConfig;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides which runtime properties are sensitive, so that their values are not exposed by the startup property
 * logging, the {@code /status/properties} endpoint, or the logged task commands.
 *
 * A property is sensitive if its name contains, ignoring case, any word in {@code druid.server.hiddenProperties}.
 * For backward compatibility, the words in the deprecated {@code druid.startup.logging.maskProperties} are honored as
 * well; if it is not set, its former default words are used, so that logs are masked at least as before.
 */
@LazySingleton
public class SensitiveProperties
{
  private static final Logger log = new Logger(SensitiveProperties.class);

  /**
   * Former default of {@code druid.startup.logging.maskProperties}.
   */
  private static final List<String> DEFAULT_MASK_PROPERTIES = ImmutableList.of("password", "secret");

  private final Set<String> lowerCaseWords = new HashSet<>();

  /**
   * @param druidServerConfig    provides {@code druid.server.hiddenProperties}; if null, its default is used
   * @param startupLoggingConfig provides the deprecated {@code druid.startup.logging.maskProperties}
   */
  @Inject
  public SensitiveProperties(
      @Nullable final DruidServerConfig druidServerConfig,
      @Nullable final StartupLoggingConfig startupLoggingConfig
  )
  {
    addWords(
        druidServerConfig == null
        ? DruidServerConfig.DEFAULT_HIDDEN_PROPERTIES
        : druidServerConfig.getHiddenProperties()
    );

    final List<String> maskProperties = startupLoggingConfig == null ? null : startupLoggingConfig.getMaskProperties();
    if (maskProperties == null) {
      addWords(DEFAULT_MASK_PROPERTIES);
    } else {
      log.warn(
          "Property [druid.startup.logging.maskProperties] is deprecated and will be removed in a future release;"
          + " its values %s are still applied. Move them to [druid.server.hiddenProperties].",
          maskProperties
      );
      addWords(maskProperties);
    }
  }

  private void addWords(final Collection<String> words)
  {
    words.forEach(word -> lowerCaseWords.add(StringUtils.toLowerCase(word)));
  }

  /**
   * Returns true if the property name contains any of the configured words, ignoring case.
   */
  public boolean isSensitive(final String propertyName)
  {
    final String lowerCaseName = StringUtils.toLowerCase(propertyName);
    return lowerCaseWords.stream().anyMatch(lowerCaseName::contains);
  }
}
