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

package org.apache.druid.iceberg.input;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents vended credentials from a catalog for accessing storage.
 * Provides typed accessors for different cloud storage credential types.
 */
public class VendedCredentials
{
  /**
   * All known credential keys, derived from each credential type's {@code allKeys()}.
   */
  private static final List<String> ALL_CREDENTIAL_KEYS = Stream.of(
      S3Credentials.allKeys(),
      GcsCredentials.allKeys(),
      AzureCredentials.allKeys()
  ).flatMap(Collection::stream).collect(Collectors.toList());

  private final Map<String, String> rawCredentials;

  public VendedCredentials(@Nullable Map<String, String> rawCredentials)
  {
    this.rawCredentials = rawCredentials;
  }

  /**
   * Extracts known credential keys from the given properties map.
   *
   * @param properties the source map to extract credentials from
   * @return VendedCredentials containing any found credentials, or null if none found
   */
  @Nullable
  public static VendedCredentials extractFrom(@Nullable Map<String, String> properties)
  {
    if (properties == null || properties.isEmpty()) {
      return null;
    }

    Map<String, String> extracted = new HashMap<>();
    for (String key : ALL_CREDENTIAL_KEYS) {
      String value = properties.get(key);
      if (value != null && !value.isEmpty()) {
        extracted.put(key, value);
      }
    }

    return extracted.isEmpty() ? null : new VendedCredentials(extracted);
  }

  /**
   * Returns the raw credential map for custom extraction.
   */
  @Nullable
  public Map<String, String> getRawCredentials()
  {
    return rawCredentials;
  }

  public boolean isEmpty()
  {
    return rawCredentials == null || rawCredentials.isEmpty();
  }

  /**
   * S3 temporary credentials from catalog vending.
   */
  public static class S3Credentials
  {
    public static final String ACCESS_KEY_ID = "s3.access-key-id";
    public static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
    public static final String SESSION_TOKEN = "s3.session-token";

    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    public S3Credentials(String accessKeyId, String secretAccessKey, @Nullable String sessionToken)
    {
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
      this.sessionToken = sessionToken;
    }

    static List<String> allKeys()
    {
      return Arrays.asList(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
    }

    @Nullable
    public static S3Credentials extract(Map<String, String> props)
    {
      String ak = props.get(ACCESS_KEY_ID);
      String sk = props.get(SECRET_ACCESS_KEY);
      if (ak == null || sk == null) {
        return null;
      }
      return new S3Credentials(ak, sk, props.get(SESSION_TOKEN));
    }

    public String getAccessKeyId()
    {
      return accessKeyId;
    }

    public String getSecretAccessKey()
    {
      return secretAccessKey;
    }

    @Nullable
    public String getSessionToken()
    {
      return sessionToken;
    }
  }

  /**
   * GCS OAuth2 credentials from catalog vending.
   */
  public static class GcsCredentials
  {
    public static final String OAUTH2_TOKEN = "gcs.oauth2.token";
    public static final String OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

    private final String oauth2Token;
    private final String expiresAt;

    public GcsCredentials(String oauth2Token, @Nullable String expiresAt)
    {
      this.oauth2Token = oauth2Token;
      this.expiresAt = expiresAt;
    }

    static List<String> allKeys()
    {
      return Arrays.asList(OAUTH2_TOKEN, OAUTH2_TOKEN_EXPIRES_AT);
    }

    @Nullable
    public static GcsCredentials extract(Map<String, String> props)
    {
      String token = props.get(OAUTH2_TOKEN);
      if (token == null) {
        return null;
      }
      return new GcsCredentials(token, props.get(OAUTH2_TOKEN_EXPIRES_AT));
    }

    public String getOauth2Token()
    {
      return oauth2Token;
    }

    @Nullable
    public String getExpiresAt()
    {
      return expiresAt;
    }
  }

  /**
   * Azure/ADLS credentials from catalog vending.
   */
  public static class AzureCredentials
  {
    public static final String SAS_TOKEN = "adls.sas-token";

    private final String sasToken;

    public AzureCredentials(String sasToken)
    {
      this.sasToken = sasToken;
    }

    static List<String> allKeys()
    {
      return Arrays.asList(SAS_TOKEN);
    }

    @Nullable
    public static AzureCredentials extract(Map<String, String> props)
    {
      String token = props.get(SAS_TOKEN);
      if (token == null) {
        return null;
      }
      return new AzureCredentials(token);
    }

    public String getSasToken()
    {
      return sasToken;
    }
  }
}
