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

package org.apache.druid.query.lookup;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.security.sasl.Sasl;
import java.util.Map;

/**
 * Checks that a lookup configured for {@code AWS_MSK_IAM} as documented logs in using only the AWS SDK on Druid's
 * core classpath. Login happens in the consumer constructor, so no broker is needed.
 */
public class AwsMskIamAuthTest
{
  private static final String LOGIN_MODULE = "software.amazon.msk.auth.iam.IAMLoginModule";

  @Test
  public void testDefaultCredentials() throws Exception
  {
    createAndCloseConsumer(LOGIN_MODULE + " required;");

    // Construction never connects, so separately check the mechanism the SASL handshake would look up.
    Assertions.assertNotNull(
        Sasl.createSaslClient(new String[]{"AWS_MSK_IAM"}, null, "kafka", "localhost", Map.of(), callbacks -> {}),
        "AWS_MSK_IAM SASL mechanism is not registered"
    );
  }

  /**
   * Assume-role builds an {@code StsClient} at login, which reaches more of the SDK than the default chain.
   */
  @Test
  public void testAssumeRole()
  {
    createAndCloseConsumer(
        LOGIN_MODULE + " required awsRoleArn=\"arn:aws:iam::123456789012:role/msk-consumer\" awsStsRegion=\"us-east-1\""
        + " awsAddDefaultProviders=\"false\";"
    );
  }

  private static void createAndCloseConsumer(final String jaasConfig)
  {
    final Map<String, String> kafkaProperties = Map.of(
        "bootstrap.servers", "localhost:9098",
        "security.protocol", "SASL_SSL",
        "sasl.mechanism", "AWS_MSK_IAM",
        "sasl.jaas.config", jaasConfig,
        "sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    );

    new KafkaLookupExtractorFactory(null, "lookup-topic", kafkaProperties).getConsumer().close();
  }
}
