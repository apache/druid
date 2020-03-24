/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import javax.annotation.Nullable;

public class TenantUtils
{
  private static final char DELIMITER = '_';

  @Nullable
  public static String extractTenant(String prefixedTopic)
  {
    int i = prefixedTopic.indexOf(DELIMITER);
    return i < 0 ? null : prefixedTopic.substring(0, i);
  }

  @Nullable
  public static String extractTenantTopic(String prefixedTopic)
  {
    int i = prefixedTopic.indexOf(DELIMITER);
    return i < 0 ? null : prefixedTopic.substring(i + 1);
  }
}
