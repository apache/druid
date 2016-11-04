/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.variance;

import com.google.common.collect.Lists;

import io.druid.java.util.common.Pair;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class VarianceAggregatorCollectorTest
{
  private static final float[] market_upfront = new float[]{
      800.0f, 800.0f, 826.0602f, 1564.6177f, 1006.4021f, 869.64374f, 809.04175f, 1458.4027f, 852.4375f, 879.9881f,
      950.1468f, 712.7746f, 846.2675f, 682.8855f, 1109.875f, 594.3817f, 870.1159f, 677.511f, 1410.2781f, 1219.4321f,
      979.306f, 1224.5016f, 1215.5898f, 716.6092f, 1301.0233f, 786.3633f, 989.9315f, 1609.0967f, 1023.2952f, 1367.6381f,
      1627.598f, 810.8894f, 1685.5001f, 545.9906f, 1870.061f, 555.476f, 1643.3408f, 943.4972f, 1667.4978f, 913.5611f,
      1218.5619f, 1273.7074f, 888.70526f, 1113.1141f, 864.5689f, 1308.582f, 785.07886f, 1363.6149f, 787.1253f,
      826.0392f, 1107.2438f, 872.6257f, 1188.3693f, 911.9568f, 794.0988f, 1299.0933f, 1212.9283f, 901.3273f, 723.5143f,
      1061.9734f, 602.97955f, 879.4061f, 724.2625f, 862.93134f, 1133.1351f, 948.65796f, 807.6017f, 914.525f, 1553.3485f,
      1208.4567f, 679.6193f, 645.1777f, 1120.0887f, 1649.5333f, 1433.3988f, 1598.1793f, 1192.5631f, 1022.85455f,
      1228.5024f, 1298.4158f, 1345.9644f, 1291.898f, 1306.4957f, 1287.7667f, 1631.5844f, 578.79596f, 1017.5732f,
      782.0135f, 829.91626f, 1862.7379f, 873.3065f, 1427.0167f, 1430.2573f, 1101.9182f, 1166.1411f, 1004.94086f,
      740.1837f, 865.7779f, 901.30756f, 691.9589f, 1674.3317f, 975.57794f, 1360.6948f, 755.89935f, 771.34845f,
      869.30835f, 1095.6376f, 906.3738f, 988.8938f, 835.76263f, 776.70294f, 875.6834f, 1070.8363f, 835.46124f,
      715.5161f, 755.64655f, 771.1005f, 764.50806f, 736.40924f, 884.8373f, 918.72284f, 893.98505f, 832.8749f,
      850.995f, 767.9733f, 848.3399f, 878.6838f, 906.1019f, 1403.8302f, 936.4296f, 846.2884f, 856.4901f, 1032.2576f,
      954.7542f, 1031.99f, 907.02155f, 1110.789f, 843.95215f, 1362.6506f, 884.8015f, 1684.2688f, 873.65204f, 855.7177f,
      996.56415f, 1061.6786f, 962.2358f, 1019.8985f, 1056.4193f, 1198.7231f, 1108.1361f, 1289.0095f,
      1069.4318f, 1001.13403f, 1030.4995f, 1734.2749f, 1063.2012f, 1447.3412f, 1234.2476f, 1144.3424f, 1049.7385f,
      811.9913f, 768.4231f, 1151.0692f, 877.0794f, 1146.4231f, 902.6157f, 1355.8434f, 897.39343f, 1260.1431f, 762.8625f,
      935.168f, 782.10785f, 996.2054f, 767.69214f, 1031.7415f, 775.9656f, 1374.9684f, 853.163f, 1456.6118f, 811.92523f,
      989.0328f, 744.7446f, 1166.4012f, 753.105f, 962.7312f, 780.272f
  };

  private static final float[] market_total_market = new float[]{
      1000.0f, 1000.0f, 1040.9456f, 1689.0128f, 1049.142f, 1073.4766f, 1007.36554f, 1545.7089f, 1016.9652f, 1077.6127f,
      1075.0896f, 953.9954f, 1022.7833f, 937.06195f, 1156.7448f, 849.8775f, 1066.208f, 904.34064f, 1240.5255f,
      1343.2325f, 1088.9431f, 1349.2544f, 1102.8667f, 939.2441f, 1109.8754f, 997.99457f, 1037.4495f, 1686.4197f,
      1074.007f, 1486.2013f, 1300.3022f, 1021.3345f, 1314.6195f, 792.32605f, 1233.4489f, 805.9301f, 1184.9207f,
      1127.231f, 1203.4656f, 1100.9048f, 1097.2112f, 1410.793f, 1033.4012f, 1283.166f, 1025.6333f, 1331.861f,
      1039.5005f, 1332.4684f, 1011.20544f, 1029.9952f, 1047.2129f, 1057.08f, 1064.9727f, 1082.7277f, 971.0508f,
      1320.6383f, 1070.1655f, 1089.6478f, 980.3866f, 1179.6959f, 959.2362f, 1092.417f, 987.0674f, 1103.4583f,
      1091.2231f, 1199.6074f, 1044.3843f, 1183.2408f, 1289.0973f, 1360.0325f, 993.59125f, 1021.07117f, 1105.3834f,
      1601.8295f, 1200.5272f, 1600.7233f, 1317.4584f, 1304.3262f, 1544.1082f, 1488.7378f, 1224.8271f, 1421.6487f,
      1251.9062f, 1414.619f, 1350.1754f, 970.7283f, 1057.4272f, 1073.9673f, 996.4337f, 1743.9218f, 1044.5629f,
      1474.5911f, 1159.2788f, 1292.5428f, 1124.2014f, 1243.354f, 1051.809f, 1143.0784f, 1097.4907f, 1010.3703f,
      1326.8291f, 1179.8038f, 1281.6012f, 994.73126f, 1081.6504f, 1103.2397f, 1177.8584f, 1152.5477f, 1117.954f,
      1084.3325f, 1029.8025f, 1121.3854f, 1244.85f, 1077.2794f, 1098.5432f, 998.65076f, 1088.8076f, 1008.74554f,
      998.75397f, 1129.7233f, 1075.243f, 1141.5884f, 1037.3811f, 1099.1973f, 981.5773f, 1092.942f, 1072.2394f,
      1154.4156f, 1311.1786f, 1176.6052f, 1107.2202f, 1102.699f, 1285.0901f, 1217.5475f, 1283.957f, 1178.8302f,
      1301.7781f, 1119.2472f, 1403.3389f, 1156.6019f, 1429.5802f, 1137.8423f, 1124.9352f, 1256.4998f, 1217.8774f,
      1247.8909f, 1185.71f, 1345.7817f, 1250.1667f, 1390.754f, 1224.1162f, 1361.0802f, 1190.9337f, 1310.7971f,
      1466.2094f, 1366.4476f, 1314.8397f, 1522.0437f, 1193.5563f, 1321.375f, 1055.7837f, 1021.6387f, 1197.0084f,
      1131.532f, 1192.1443f, 1154.2896f, 1272.6771f, 1141.5146f, 1190.8961f, 1009.36316f, 1006.9138f, 1032.5999f,
      1137.3857f, 1030.0756f, 1005.25305f, 1030.0947f, 1112.7948f, 1113.3575f, 1153.9747f, 1069.6409f, 1016.13745f,
      994.9023f, 1032.1543f, 999.5864f, 994.75275f, 1029.057f
  };

  @Test
  public void testVariance()
  {
    Random random = new Random();
    for (float[] values : Arrays.asList(market_upfront, market_total_market)) {
      double sum = 0;
      for (float f : values) {
        sum += f;
      }
      final double mean = sum / values.length;
      double temp = 0;
      for (float f : values) {
        temp += Math.pow(f - mean, 2);
      }

      final double variance_pop = temp / values.length;
      final double variance_sample = temp / (values.length - 1);

      VarianceAggregatorCollector holder = new VarianceAggregatorCollector();
      for (float f : values) {
        holder.add(f);
      }
      Assert.assertEquals(holder.getVariance(true), variance_pop, 0.001);
      Assert.assertEquals(holder.getVariance(false), variance_sample, 0.001);

      for (int mergeOn : new int[] {2, 3, 5, 9}) {
        List<VarianceAggregatorCollector> holders1 = Lists.newArrayListWithCapacity(mergeOn);
        List<Pair<VarianceBufferAggregator, ByteBuffer>> holders2 = Lists.newArrayListWithCapacity(mergeOn);

        FloatHandOver valueHandOver = new FloatHandOver();
        for (int i = 0; i < mergeOn; i++) {
          holders1.add(new VarianceAggregatorCollector());
          holders2.add(Pair.<VarianceBufferAggregator, ByteBuffer>of(
                           new VarianceBufferAggregator.FloatVarianceAggregator("XX", valueHandOver),
                           ByteBuffer.allocate(VarianceAggregatorCollector.getMaxIntermediateSize())
                       ));
        }
        for (float f : values) {
          valueHandOver.v = f;
          int index = random.nextInt(mergeOn);
          holders1.get(index).add(f);
          holders2.get(index).lhs.aggregate(holders2.get(index).rhs, 0);
        }
        VarianceAggregatorCollector holder1 = holders1.get(0);
        for (int i = 1; i < mergeOn; i++) {
          holder1 = (VarianceAggregatorCollector) VarianceAggregatorCollector.combineValues(holder1, holders1.get(i));
        }
        ObjectHandOver collectHandOver = new ObjectHandOver();
        ByteBuffer buffer = ByteBuffer.allocate(VarianceAggregatorCollector.getMaxIntermediateSize());
        VarianceBufferAggregator.ObjectVarianceAggregator merger = new VarianceBufferAggregator.ObjectVarianceAggregator("xxx", collectHandOver);
        for (int i = 0; i < mergeOn; i++) {
          collectHandOver.v = holders2.get(i).lhs.get(holders2.get(i).rhs, 0);
          merger.aggregate(buffer, 0);
        }
        VarianceAggregatorCollector holder2 = (VarianceAggregatorCollector) merger.get(buffer, 0);
        Assert.assertEquals(holder2.getVariance(true), variance_pop, 0.01);
        Assert.assertEquals(holder2.getVariance(false), variance_sample, 0.01);
      }
    }
  }

  private static class FloatHandOver implements FloatColumnSelector
  {
    float v;

    @Override
    public float get()
    {
      return v;
    }
  }

  private static class ObjectHandOver implements ObjectColumnSelector
  {
    Object v;

    @Override
    public Class classOfObject()
    {
      return v == null ? Object.class : v.getClass();
    }

    @Override
    public Object get()
    {
      return v;
    }
  }
}
