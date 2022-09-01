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

import com.google.common.net.InetAddresses;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.expression.IPv4AddressExprUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.annotation.Nullable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IPv4AddressBenchmark
{
  private static final Pattern IPV4_PATTERN = Pattern.compile(
      "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
  );
  // Different representations of IPv4 addresses.
  private List<String> inputStrings;
  private List<Long> inputLongs;
  private List<IPv4Address> addresses;
  private List<Inet4Address> inet4Addresses;

  @Param({"100000"})
  public int numOfAddresses;

  @Param({"16", "31"})
  public int prefixRange;

  SubnetUtils.SubnetInfo subnetInfo;
  IPAddressString subnetString;
  IPAddress subnetBlock;


  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(IPv4AddressBenchmark.class.getSimpleName())
        .forks(1)
        .build();
    new Runner(opt).run();
  }

  @Setup
  public void setUp()
  {
    inputStrings = new ArrayList<>(numOfAddresses);
    addresses = new ArrayList<>(numOfAddresses);
    inet4Addresses = new ArrayList<>(numOfAddresses);
    inputLongs = new ArrayList<>(numOfAddresses);

    Random r = ThreadLocalRandom.current();
    String subnetAddress = generateIpAddressString(r) + "/" + prefixRange;

    try {
      subnetString = new IPAddressString(subnetAddress);
      subnetBlock = subnetString.toAddress().toPrefixBlock();
      subnetInfo = getSubnetInfo(subnetAddress);
    }
    catch (AddressStringException e) {
      throw new RuntimeException(e);
    }

    for (int i = 0; i < numOfAddresses; i++) {
      String genIpAddress = generateIpAddressString(r);
      IPAddressString ipAddressString = new IPAddressString(genIpAddress);
      inputStrings.add(ipAddressString.toString());

      IPv4Address iPv4Address = ipAddressString.getAddress().toIPv4();

      addresses.add(iPv4Address);
      inet4Addresses.add(iPv4Address.toInetAddress());
      inputLongs.add(iPv4Address.longValue());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void stringContainsUsingIpAddrPrefixContains(Blackhole blackhole)
  {
    for (String v4Address : inputStrings) {
      final IPAddressString iPv4Address = IPv4AddressExprUtils.parseString(v4Address);
      blackhole.consume(iPv4Address != null && subnetString.prefixContains(iPv4Address));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void stringContainsUsingIpAddrPrefixContainsTooManyRoundTrips(Blackhole blackhole)
  {
    for (String v4Address : inputStrings) {
      final IPv4Address iPv4Address = IPv4AddressExprUtils.parse(v4Address);
      blackhole.consume(iPv4Address != null && subnetString.prefixContains(iPv4Address.toAddressString()));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void stringContainsUsingSubnetUtils(Blackhole blackhole) throws IAE
  {
    for (String v4Address : inputStrings) {
      blackhole.consume(isValidAddress(v4Address) && subnetInfo.isInRange(v4Address));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parseLongUsingIpAddr(Blackhole blackhole)
  {
    for (Long v4Address : inputLongs) {
      blackhole.consume(IPv4AddressExprUtils.parse(v4Address));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parseLongUsingSubnetUtils(Blackhole blackhole)
  {
    for (Long v4Address : inputLongs) {
      blackhole.consume(parseUsingSubnetUtils(v4Address));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void toLongUsingSubnetUtils(Blackhole blackhole)
  {
    for (Inet4Address v4InetAddress : inet4Addresses) {
      blackhole.consume(toLongUsingSubnetUtils(v4InetAddress));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void toLongUsingIpAddr(Blackhole blackhole)
  {
    for (IPv4Address v4Address : addresses) {
      blackhole.consume(IPv4AddressExprUtils.toLong(v4Address));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void longContainsUsingSubnetUtils(Blackhole blackhole)
  {
    for (long v4Long : inputLongs) {
      blackhole.consume(!IPv4AddressExprUtils.overflowsUnsignedInt(v4Long) && subnetInfo.isInRange((int) v4Long));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void longContainsUsingIpAddr(Blackhole blackhole)
  {
    for (long v4Long : inputLongs) {
      final IPv4Address iPv4Address = IPv4AddressExprUtils.parse(v4Long);
      blackhole.consume(iPv4Address != null && subnetBlock.contains(iPv4Address));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void longContainsUsingIpAddrWithStrings(Blackhole blackhole)
  {
    for (long v4Long : inputLongs) {
      final IPv4Address iPv4Address = IPv4AddressExprUtils.parse(v4Long);
      blackhole.consume(iPv4Address != null && subnetString.prefixContains(iPv4Address.toAddressString()));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parseStringUsingIpAddr(Blackhole blackhole)
  {
    for (String ipv4Addr : inputStrings) {
      blackhole.consume(IPv4AddressExprUtils.parse(ipv4Addr));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parseStringUsingIpAddrString(Blackhole blackhole)
  {
    for (String ipv4Addr : inputStrings) {
      blackhole.consume(IPv4AddressExprUtils.parseString(ipv4Addr));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void parseStringUsingSubnetUtils(Blackhole blackhole)
  {
    for (String ipv4Addr : inputStrings) {
      blackhole.consume(parseUsingSubnetUtils(ipv4Addr));
    }
  }

  private static long toLongUsingSubnetUtils(Inet4Address address)
  {
    int value = InetAddresses.coerceToInteger(address);
    return Integer.toUnsignedLong(value);
  }

  @Nullable
  private static Inet4Address parseUsingSubnetUtils(String string)
  {
    if (isValidAddress(string)) {
      InetAddress address = InetAddresses.forString(string);
      if (address instanceof Inet4Address) {
        return (Inet4Address) address;
      }
    }
    return null;
  }

  private static Inet4Address parseUsingSubnetUtils(long longValue)
  {
    if (IPv4AddressExprUtils.overflowsUnsignedInt(longValue)) {
      return InetAddresses.fromInteger((int) longValue);
    }
    return null;
  }

  private static SubnetUtils.SubnetInfo getSubnetInfo(String subnet)
  {
    SubnetUtils subnetUtils;
    try {
      subnetUtils = new SubnetUtils(subnet);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(e, "getSubnetInfo() arg has an invalid format: " + subnet);
    }
    subnetUtils.setInclusiveHostCount(true);  // make network and broadcast addresses match
    return subnetUtils.getInfo();
  }


  static boolean isValidAddress(@Nullable String string)
  {
    return string != null && IPV4_PATTERN.matcher(string).matches();
  }

  private static String generateIpAddressString(Random r)
  {
    return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
  }
}
