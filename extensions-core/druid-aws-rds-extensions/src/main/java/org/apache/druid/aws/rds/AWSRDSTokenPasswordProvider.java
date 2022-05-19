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

package org.apache.druid.aws.rds;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;

/**
 * Generates the AWS token same as aws cli
 * aws rds generate-db-auth-token --hostname HOST --port PORT --region REGION --username USER
 * and returns that as password.
 *
 * Before using this, please make sure that you have connected all dots for db user to connect using token.
 * See https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html
 */
public class AWSRDSTokenPasswordProvider implements PasswordProvider
{
  private static final Logger LOGGER = new Logger(AWSRDSTokenPasswordProvider.class);
  private final String user;
  private final String host;
  private final int port;
  private final String region;

  private final AWSCredentialsProvider awsCredentialsProvider;

  @JsonCreator
  public AWSRDSTokenPasswordProvider(
      @JsonProperty("user") String user,
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("region") String region,
      @JacksonInject AWSCredentialsProvider awsCredentialsProvider
  )
  {
    this.user = Preconditions.checkNotNull(user, "null metadataStorage user");
    this.host = Preconditions.checkNotNull(host, "null metadataStorage host");
    Preconditions.checkArgument(port > 0, "must provide port");
    this.port = port;

    this.region = Preconditions.checkNotNull(region, "null region");

    LOGGER.info("AWS RDS Config user[%s], host[%s], port[%d], region[%s]", this.user, this.host, port, this.region);
    this.awsCredentialsProvider = Preconditions.checkNotNull(awsCredentialsProvider, "null AWSCredentialsProvider");
  }

  @JsonProperty
  public String getUser()
  {
    return user;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public String getRegion()
  {
    return region;
  }

  @JsonIgnore
  @Override
  public String getPassword()
  {
    try {
      RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator
          .builder()
          .credentials(awsCredentialsProvider)
          .region(region)
          .build();

      String authToken = generator.getAuthToken(
          GetIamAuthTokenRequest
              .builder()
              .hostname(host)
              .port(port)
              .userName(user)
              .build()
      );

      return authToken;
    }
    catch (Exception ex) {
      LOGGER.error(ex, "Couldn't generate AWS token.");
      throw new RE(ex, "Couldn't generate AWS token.");
    }
  }
}
