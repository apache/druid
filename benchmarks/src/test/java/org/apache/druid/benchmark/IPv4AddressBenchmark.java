package org.apache.druid.benchmark;

import java.util.ArrayList;
import java.util.Random;

import com.google.common.net.InetAddresses;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import inet.ipaddr.ipv4.IPv4Address;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.expression.ExprUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.commons.net.util.SubnetUtils;

import javax.annotation.Nullable;
import java.net.Inet4Address;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class IPv4AddressBenchmark
{
  private static final IPAddressStringParameters IPV4_ADDRESS_PARAMS = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowIPv6(false).allowPrefix(false).allowEmpty(false).toParams();

  private static List<String> IPV4_ADDRESSES;
  private static List<String> IPV4_SUBNETS;

  @Param({"10", "100", "1000"})
  public int numOfAddresses;

  @Setup
  public void setUp()
  {
    IPV4_ADDRESSES = new ArrayList<>(numOfAddresses);
    IPV4_SUBNETS = new ArrayList<>(numOfAddresses);

    for (int i = 0; i < numOfAddresses; i++) {
      Random r = new Random();
      String genIpAddress = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
      IPAddressString ipAddressString = new IPAddressString(genIpAddress);
      IPAddress prefixBlock = ipAddressString.getAddress().applyPrefixLength(r.nextInt(32)).toPrefixBlock();
      IPV4_ADDRESSES.add(ipAddressString.toString());
      IPV4_SUBNETS.add(prefixBlock.toString());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void stringContainsUsingIpAddr()
  {
    for (int i = 0; i < IPV4_ADDRESSES.size(); i++) {
      String v4Subnet = IPV4_SUBNETS.get(i);
      String v4Address = IPV4_ADDRESSES.get(i);

      IPAddressString subnetString = new IPAddressString(v4Subnet);
      IPv4Address iPv4Address = IPv4AddressExprUtils.parse(v4Address);
      if (iPv4Address != null) {
        subnetString.contains(iPv4Address.toAddressString());
      }
    }
  }

  static long parseStrToLong(@Nullable String string)
  {
    IPAddressString ipAddressString = new IPAddressString(string, IPV4_ADDRESS_PARAMS);
    if (ipAddressString.isIPv4()) {
      return ipAddressString.getAddress().toIPv4().longValue();
    }
    return -1;
  }

  static IPv4Address parseStrToIpAddress(@Nullable String string)
  {
    IPAddressString ipAddressString = new IPAddressString(string, IPV4_ADDRESS_PARAMS);
    if (ipAddressString.isIPv4()) {
      return ipAddressString.getAddress().toIPv4();
    }
    return null;
  }

  private static final Pattern IPV4_PATTERN = Pattern.compile(
      "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
  );

  static boolean isValidAddress(String string)
  {
    return string != null && IPV4_PATTERN.matcher(string).matches();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void stringContainsUsingSubnetUtils() throws IAE
  {
    for (int i = 0; i < IPV4_ADDRESSES.size(); i++) {
      String v4Subnet = IPV4_SUBNETS.get(i);
      String v4Address = IPV4_ADDRESSES.get(i);

      SubnetUtils.SubnetInfo subnetInfo = getSubnetInfo(v4Subnet);
      if (isValidAddress(v4Address)) {
        subnetInfo.isInRange(v4Address);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseLongUsingIpAddr()
  {
    for (String v4Address : IPV4_ADDRESSES) {
      long v4AddressLong = parseStrToLong(v4Address);
      IPv4AddressExprUtils.parse(v4AddressLong);
    }
  }

  private Inet4Address parseUsingSubnetUtils(long longValue)
  {
    if(IPv4AddressExprUtils.overflowsUnsignedInt(longValue))
    {
      return InetAddresses.fromInteger((int) longValue);
    }
    return null;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseLongUsingSubnetUtils()
  {
    for (String v4Address : IPV4_ADDRESSES) {
      long v4AddressLong = parseStrToLong(v4Address);
      parseUsingSubnetUtils(v4AddressLong);
    }
  }

  private long toLongUsingSubnetUtils(Inet4Address address)
  {
    int value = InetAddresses.coerceToInteger(address);
    return Integer.toUnsignedLong(value);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseToLongUsingSubnetUtils()
  {
    for (String v4Address : IPV4_ADDRESSES) {
      IPv4Address iPv4Address = parseStrToIpAddress(v4Address);
      toLongUsingSubnetUtils(iPv4Address.toInetAddress());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseToLongUsingIpAddr()
  {
    for (String v4Address : IPV4_ADDRESSES) {
      IPv4Address iPv4Address = parseStrToIpAddress(v4Address);
      IPv4AddressExprUtils.toLong(iPv4Address);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void longContainsUsingSubnetUtils()
  {
    for (int i = 0; i < IPV4_ADDRESSES.size(); i++) {
      String v4Subnet = IPV4_SUBNETS.get(i);
      String v4Address = IPV4_ADDRESSES.get(i);

      IPv4Address iPv4Address = parseStrToIpAddress(v4Address);
      long v4Long = toLongUsingSubnetUtils(iPv4Address.toInetAddress());
      SubnetUtils.SubnetInfo subnetInfo = getSubnetInfo(v4Subnet);
      if (!IPv4AddressExprUtils.overflowsUnsignedInt(v4Long)) {
        subnetInfo.isInRange((int) v4Long);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void longContainsUsingIpAddr()
  {
    for (int i = 0; i < IPV4_ADDRESSES.size(); i++) {
      String v4Subnet = IPV4_SUBNETS.get(i);
      String v4Address = IPV4_ADDRESSES.get(i);

      IPv4Address iPv4Address = parseStrToIpAddress(v4Address);
      IPAddressString subnetString = new IPAddressString(v4Subnet);
      if(iPv4Address != null) {
        subnetString.contains(iPv4Address.toAddressString());
      }
    }
  }

  private SubnetUtils.SubnetInfo getSubnetInfo(String subnet)
   {
     SubnetUtils subnetUtils;
     try {
       subnetUtils = new SubnetUtils(subnet);
     }
     catch (IllegalArgumentException e) {
       throw new IAE(e, ExprUtils.createErrMsg("getSubnetInfo()",   " arg has an invalid format: " + subnet));
     }
     subnetUtils.setInclusiveHostCount(true);  // make network and broadcast addresses match
     return subnetUtils.getInfo();
   }


  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(IPv4AddressBenchmark.class.getSimpleName())
        .forks(1)
        .build();
    new Runner(opt).run();
  }
}
