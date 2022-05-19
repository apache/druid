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

package org.apache.druid.benchmark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class BasicAuthUserMapSerdeBenchmark
{
  @Param({"1000"})
  private int numUsers;

  private ObjectMapper smileMapper;
  private Map<String, BenchmarkUser> userMap;
  private List<byte[]> serializedUsers;

  @Setup
  public void setup() throws IOException
  {
    smileMapper = new ObjectMapper(new SmileFactory());
    userMap = new HashMap<>();
    for (int i = 0; i < numUsers; i++) {
      BenchmarkUser user = makeUser();
      userMap.put(user.getName(), user);
    }

    serializedUsers = new ArrayList<>();
    for (BenchmarkUser user : userMap.values()) {
      byte[] serializedUser = smileMapper.writeValueAsBytes(user);
      serializedUsers.add(serializedUser);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void serialize(Blackhole blackhole) throws Exception
  {
    for (BenchmarkUser user : userMap.values()) {
      byte[] serializedUser = smileMapper.writeValueAsBytes(user);
      blackhole.consume(serializedUser);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void deserialize(Blackhole blackhole) throws Exception
  {
    for (byte[] serializedUser : serializedUsers) {
      BenchmarkUser user = smileMapper.readValue(serializedUser, BenchmarkUser.class);
      blackhole.consume(user);
    }
  }

  private BenchmarkUser makeUser()
  {
    byte[] salt = new byte[32];
    byte[] hash = new byte[64];

    Random random = ThreadLocalRandom.current();
    random.nextBytes(salt);
    random.nextBytes(hash);
    return new BenchmarkUser(UUID.randomUUID().toString(), new BenchmarkCredentials(salt, hash, 10000));
  }

  private static class BenchmarkUser
  {
    private final String name;
    private final BenchmarkCredentials credentials;

    @JsonCreator
    public BenchmarkUser(
        @JsonProperty("name") String name,
        @JsonProperty("credentials") BenchmarkCredentials credentials
    )
    {
      this.name = name;
      this.credentials = credentials;
    }

    @JsonProperty
    public String getName()
    {
      return name;
    }

    @JsonProperty
    public BenchmarkCredentials getCredentials()
    {
      return credentials;
    }
  }

  private static class BenchmarkCredentials
  {
    private final byte[] salt;
    private final byte[] hash;
    private final int iterations;

    @JsonCreator
    public BenchmarkCredentials(
        @JsonProperty("salt") byte[] salt,
        @JsonProperty("hash") byte[] hash,
        @JsonProperty("iterations") int iterations
    )
    {
      this.salt = salt;
      this.hash = hash;
      this.iterations = iterations;
    }

    @JsonProperty
    public byte[] getSalt()
    {
      return salt;
    }

    @JsonProperty
    public byte[] getHash()
    {
      return hash;
    }

    @JsonProperty
    public int getIterations()
    {
      return iterations;
    }
  }
}
