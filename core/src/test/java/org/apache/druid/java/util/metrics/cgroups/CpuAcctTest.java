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

package org.apache.druid.java.util.metrics.cgroups;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.LongStream;


public class CpuAcctTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File cpuacctDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );
    Assert.assertTrue((cpuacctDir.isDirectory() && cpuacctDir.exists()) || cpuacctDir.mkdirs());
    TestUtils.copyResource("/cpuacct.usage_all", new File(cpuacctDir, "cpuacct.usage_all"));
  }

  @Test
  public void testWontCrash()
  {
    final CpuAcct cpuAcct = new CpuAcct(cgroup -> {
      throw new RuntimeException("Should still continue");
    });
    final CpuAcct.CpuAcctMetric metric = cpuAcct.snapshot();
    Assert.assertEquals(0L, metric.cpuCount());
    Assert.assertEquals(0L, metric.usrTime());
    Assert.assertEquals(0L, metric.sysTime());
  }

  @Test
  public void testSimpleLoad()
  {
    final CpuAcct cpuAcct = new CpuAcct(discoverer);
    final CpuAcct.CpuAcctMetric snapshot = cpuAcct.snapshot();
    Assert.assertEquals(128, snapshot.cpuCount());
    Assert.assertArrayEquals(new long[]{
        7344294132655L,
        28183572804378L,
        29552215219002L,
        29478124053329L,
        29829248571038L,
        30290864470719L,
        30561719193413L,
        30638606697446L,
        39251561450889L,
        39082643428276L,
        38829852195583L,
        39158341842449L,
        39490263697181L,
        39363774325162L,
        39569806302164L,
        39410558504372L,
        44907796060505L,
        42522297123640L,
        41920625622542L,
        40593391967420L,
        40350585953295L,
        40139554930678L,
        40019783380923L,
        40182686097717L,
        39778858132385L,
        40252938541440L,
        40476150948365L,
        40277874584618L,
        39938509407084L,
        39914644718371L,
        40010393213659L,
        39938252119551L,
        44958993952996L,
        42967015146867L,
        41742610896758L,
        40751067975683L,
        40390633464986L,
        40143331504478L,
        40486014164571L,
        40565630824649L,
        39976774290845L,
        39942348143441L,
        40149234675554L,
        39895306827546L,
        40062736204343L,
        39208930836306L,
        40098687814379L,
        39803234124100L,
        44894501101599L,
        43470418903266L,
        41844924510711L,
        41137017142223L,
        40958534485692L,
        40996749346830L,
        40722256755299L,
        40715123538100L,
        40756697196452L,
        40388351638364L,
        40607150623932L,
        40799783862688L,
        41085552637672L,
        40406189914954L,
        40723714534227L,
        40594766265305L,
        47966186930606L,
        40950398764685L,
        39773685629470L,
        39799299693868L,
        39962809136735L,
        39621597321912L,
        39576312003193L,
        39306677714061L,
        37450385749152L,
        37262591956707L,
        37867848418162L,
        37583170923549L,
        37565074790371L,
        37490674520644L,
        37627356285158L,
        37841963931932L,
        36467248910690L,
        37168392893625L,
        37299551044970L,
        37765703017416L,
        37799573327332L,
        38049895238765L,
        37985869086888L,
        37696241330128L,
        38292683839783L,
        38120890685615L,
        38045845683675L,
        38182343881607L,
        37729375994055L,
        38074905443126L,
        37912923241296L,
        37937307782462L,
        36325058440018L,
        37157290185847L,
        37403692187351L,
        38153199365119L,
        37880374831086L,
        37651504251556L,
        37739944955714L,
        37627835848111L,
        37903369827551L,
        37981555620129L,
        37848203152449L,
        37990323769817L,
        38347560243684L,
        37887959856632L,
        37937702600487L,
        38221455324656L,
        37035158753494L,
        37519359498531L,
        38185495941617L,
        38947633192125L,
        38497334926906L,
        38621231881393L,
        38817038222494L,
        38911615674430L,
        38384669525324L,
        38597980524270L,
        38477107776771L,
        38483564156449L,
        38471547310020L,
        38827188957783L,
        38554167083817L,
        38870461161179L
    }, snapshot.usrTimes());
    Assert.assertArrayEquals(new long[]{
        4583688852335L,
        385888457233L,
        370465239852L,
        363572894675L,
        334329517808L,
        327800567541L,
        289395249935L,
        302853990791L,
        255558344564L,
        274043522998L,
        256014012773L,
        253276920707L,
        257971375228L,
        244926573383L,
        240692265222L,
        241820386298L,
        74203786299L,
        79965589957L,
        73735621808L,
        74851270413L,
        84689908189L,
        69618323977L,
        76316055513L,
        87434816528L,
        80680588218L,
        70083776283L,
        59931280896L,
        67678277638L,
        85713335794L,
        74594785949L,
        69367790895L,
        68817875729L,
        83346318662L,
        80872459027L,
        69882000641L,
        59230207177L,
        80985355290L,
        88305162767L,
        79610055475L,
        77097429500L,
        72748340407L,
        77647202034L,
        61982641775L,
        63292828955L,
        71501429739L,
        101050648913L,
        67603152691L,
        85242844849L,
        53320735254L,
        59480233446L,
        53738738094L,
        50064771695L,
        49322497528L,
        60437383202L,
        58974386647L,
        57254872107L,
        59214245666L,
        60135823463L,
        53295222550L,
        54850380995L,
        55260978656L,
        58478426264L,
        54870256138L,
        57541909382L,
        563254758724L,
        310547129324L,
        297956013630L,
        314333221636L,
        301425507083L,
        307849203392L,
        302818464346L,
        300708730620L,
        201706674463L,
        237643132530L,
        203202644049L,
        200889116684L,
        211272183842L,
        211346947120L,
        204440036165L,
        214268740497L,
        79712665639L,
        75090730135L,
        78826984017L,
        80223883420L,
        74309961837L,
        73501632933L,
        84726122364L,
        89432904192L,
        86169574674L,
        61631022854L,
        63064271269L,
        71350572902L,
        79603442903L,
        80632248259L,
        73143984225L,
        68368595815L,
        93212419757L,
        79875769874L,
        71939175528L,
        61279926839L,
        80531076242L,
        97919090064L,
        62379356509L,
        73687439569L,
        65237344029L,
        88874846168L,
        73032242506L,
        66715221936L,
        75213471783L,
        85339287415L,
        70516346051L,
        80265423715L,
        55020848133L,
        68103451501L,
        56282008328L,
        57420205172L,
        51517654341L,
        53668755335L,
        58390679981L,
        59254129440L,
        60781018005L,
        60445939750L,
        62771597837L,
        57294449683L,
        57716404007L,
        55643587481L,
        53593212339L,
        52866253864L
    }, snapshot.sysTimes());
    Assert.assertEquals(LongStream.of(snapshot.sysTimes()).sum(), snapshot.sysTime());
    Assert.assertEquals(LongStream.of(snapshot.usrTimes()).sum(), snapshot.usrTime());
    Assert.assertEquals(
        LongStream.of(snapshot.sysTimes()).sum() + LongStream.of(snapshot.usrTimes()).sum(),
        snapshot.time()
    );
  }

  @Test
  public void testSimpleMetricFunctions()
  {
    final long[] usrTime = new long[]{1, 2, 3};
    final long[] sysTime = new long[]{4, 5, 6};
    final CpuAcct.CpuAcctMetric metric = new CpuAcct.CpuAcctMetric(usrTime, sysTime);
    Assert.assertEquals(6, metric.usrTime());
    Assert.assertEquals(15, metric.sysTime());
    Assert.assertArrayEquals(usrTime, metric.usrTimes());
    Assert.assertArrayEquals(sysTime, metric.sysTimes());
    for (int i = 0; i < usrTime.length; ++i) {
      Assert.assertEquals(usrTime[i], metric.usrTime(i));
    }
    for (int i = 0; i < sysTime.length; ++i) {
      Assert.assertEquals(sysTime[i], metric.sysTime(i));
    }
  }

  @Test
  public void testDiff()
  {
    final Random random = new Random(364781L);
    final long[] zeroes = new long[32];
    Arrays.fill(zeroes, 0);
    final long[] usr = new long[zeroes.length];
    final long[] sys = new long[zeroes.length];
    long total = 0L;
    for (int i = 0; i < usr.length; ++i) {
      int add = random.nextInt(Integer.MAX_VALUE >>> 2);
      usr[i] = add;
      sys[i] = add << 1;
      total += add;
    }
    final CpuAcct.CpuAcctMetric metric0 = new CpuAcct.CpuAcctMetric(zeroes, zeroes);
    final CpuAcct.CpuAcctMetric metric1 = new CpuAcct.CpuAcctMetric(usr, sys);
    final CpuAcct.CpuAcctMetric diff = metric1.cumulativeSince(metric0);
    Assert.assertEquals(total, diff.usrTime());
    Assert.assertEquals(total << 1, diff.sysTime());
    Assert.assertNotEquals(0, total);
    final CpuAcct.CpuAcctMetric zeroDiff = metric1.cumulativeSince(metric1);
    Assert.assertEquals(0, zeroDiff.usrTime());
    Assert.assertEquals(0, zeroDiff.sysTime());
  }
}
