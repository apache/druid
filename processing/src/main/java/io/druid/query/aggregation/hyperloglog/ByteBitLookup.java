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

package io.druid.query.aggregation.hyperloglog;

/**
 */
public class ByteBitLookup
{
  public static final byte[] lookup;

  static {
    lookup = new byte[256];

    lookup[0] = 0;
    lookup[1] = 1;
    lookup[2] = 2;
    lookup[3] = 1;
    lookup[4] = 3;
    lookup[5] = 1;
    lookup[6] = 2;
    lookup[7] = 1;
    lookup[8] = 4;
    lookup[9] = 1;
    lookup[10] = 2;
    lookup[11] = 1;
    lookup[12] = 3;
    lookup[13] = 1;
    lookup[14] = 2;
    lookup[15] = 1;
    lookup[16] = 5;
    lookup[17] = 1;
    lookup[18] = 2;
    lookup[19] = 1;
    lookup[20] = 3;
    lookup[21] = 1;
    lookup[22] = 2;
    lookup[23] = 1;
    lookup[24] = 4;
    lookup[25] = 1;
    lookup[26] = 2;
    lookup[27] = 1;
    lookup[28] = 3;
    lookup[29] = 1;
    lookup[30] = 2;
    lookup[31] = 1;
    lookup[32] = 6;
    lookup[33] = 1;
    lookup[34] = 2;
    lookup[35] = 1;
    lookup[36] = 3;
    lookup[37] = 1;
    lookup[38] = 2;
    lookup[39] = 1;
    lookup[40] = 4;
    lookup[41] = 1;
    lookup[42] = 2;
    lookup[43] = 1;
    lookup[44] = 3;
    lookup[45] = 1;
    lookup[46] = 2;
    lookup[47] = 1;
    lookup[48] = 5;
    lookup[49] = 1;
    lookup[50] = 2;
    lookup[51] = 1;
    lookup[52] = 3;
    lookup[53] = 1;
    lookup[54] = 2;
    lookup[55] = 1;
    lookup[56] = 4;
    lookup[57] = 1;
    lookup[58] = 2;
    lookup[59] = 1;
    lookup[60] = 3;
    lookup[61] = 1;
    lookup[62] = 2;
    lookup[63] = 1;
    lookup[64] = 7;
    lookup[65] = 1;
    lookup[66] = 2;
    lookup[67] = 1;
    lookup[68] = 3;
    lookup[69] = 1;
    lookup[70] = 2;
    lookup[71] = 1;
    lookup[72] = 4;
    lookup[73] = 1;
    lookup[74] = 2;
    lookup[75] = 1;
    lookup[76] = 3;
    lookup[77] = 1;
    lookup[78] = 2;
    lookup[79] = 1;
    lookup[80] = 5;
    lookup[81] = 1;
    lookup[82] = 2;
    lookup[83] = 1;
    lookup[84] = 3;
    lookup[85] = 1;
    lookup[86] = 2;
    lookup[87] = 1;
    lookup[88] = 4;
    lookup[89] = 1;
    lookup[90] = 2;
    lookup[91] = 1;
    lookup[92] = 3;
    lookup[93] = 1;
    lookup[94] = 2;
    lookup[95] = 1;
    lookup[96] = 6;
    lookup[97] = 1;
    lookup[98] = 2;
    lookup[99] = 1;
    lookup[100] = 3;
    lookup[101] = 1;
    lookup[102] = 2;
    lookup[103] = 1;
    lookup[104] = 4;
    lookup[105] = 1;
    lookup[106] = 2;
    lookup[107] = 1;
    lookup[108] = 3;
    lookup[109] = 1;
    lookup[110] = 2;
    lookup[111] = 1;
    lookup[112] = 5;
    lookup[113] = 1;
    lookup[114] = 2;
    lookup[115] = 1;
    lookup[116] = 3;
    lookup[117] = 1;
    lookup[118] = 2;
    lookup[119] = 1;
    lookup[120] = 4;
    lookup[121] = 1;
    lookup[122] = 2;
    lookup[123] = 1;
    lookup[124] = 3;
    lookup[125] = 1;
    lookup[126] = 2;
    lookup[127] = 1;
    lookup[128] = 8;
    lookup[129] = 1;
    lookup[130] = 2;
    lookup[131] = 1;
    lookup[132] = 3;
    lookup[133] = 1;
    lookup[134] = 2;
    lookup[135] = 1;
    lookup[136] = 4;
    lookup[137] = 1;
    lookup[138] = 2;
    lookup[139] = 1;
    lookup[140] = 3;
    lookup[141] = 1;
    lookup[142] = 2;
    lookup[143] = 1;
    lookup[144] = 5;
    lookup[145] = 1;
    lookup[146] = 2;
    lookup[147] = 1;
    lookup[148] = 3;
    lookup[149] = 1;
    lookup[150] = 2;
    lookup[151] = 1;
    lookup[152] = 4;
    lookup[153] = 1;
    lookup[154] = 2;
    lookup[155] = 1;
    lookup[156] = 3;
    lookup[157] = 1;
    lookup[158] = 2;
    lookup[159] = 1;
    lookup[160] = 6;
    lookup[161] = 1;
    lookup[162] = 2;
    lookup[163] = 1;
    lookup[164] = 3;
    lookup[165] = 1;
    lookup[166] = 2;
    lookup[167] = 1;
    lookup[168] = 4;
    lookup[169] = 1;
    lookup[170] = 2;
    lookup[171] = 1;
    lookup[172] = 3;
    lookup[173] = 1;
    lookup[174] = 2;
    lookup[175] = 1;
    lookup[176] = 5;
    lookup[177] = 1;
    lookup[178] = 2;
    lookup[179] = 1;
    lookup[180] = 3;
    lookup[181] = 1;
    lookup[182] = 2;
    lookup[183] = 1;
    lookup[184] = 4;
    lookup[185] = 1;
    lookup[186] = 2;
    lookup[187] = 1;
    lookup[188] = 3;
    lookup[189] = 1;
    lookup[190] = 2;
    lookup[191] = 1;
    lookup[192] = 7;
    lookup[193] = 1;
    lookup[194] = 2;
    lookup[195] = 1;
    lookup[196] = 3;
    lookup[197] = 1;
    lookup[198] = 2;
    lookup[199] = 1;
    lookup[200] = 4;
    lookup[201] = 1;
    lookup[202] = 2;
    lookup[203] = 1;
    lookup[204] = 3;
    lookup[205] = 1;
    lookup[206] = 2;
    lookup[207] = 1;
    lookup[208] = 5;
    lookup[209] = 1;
    lookup[210] = 2;
    lookup[211] = 1;
    lookup[212] = 3;
    lookup[213] = 1;
    lookup[214] = 2;
    lookup[215] = 1;
    lookup[216] = 4;
    lookup[217] = 1;
    lookup[218] = 2;
    lookup[219] = 1;
    lookup[220] = 3;
    lookup[221] = 1;
    lookup[222] = 2;
    lookup[223] = 1;
    lookup[224] = 6;
    lookup[225] = 1;
    lookup[226] = 2;
    lookup[227] = 1;
    lookup[228] = 3;
    lookup[229] = 1;
    lookup[230] = 2;
    lookup[231] = 1;
    lookup[232] = 4;
    lookup[233] = 1;
    lookup[234] = 2;
    lookup[235] = 1;
    lookup[236] = 3;
    lookup[237] = 1;
    lookup[238] = 2;
    lookup[239] = 1;
    lookup[240] = 5;
    lookup[241] = 1;
    lookup[242] = 2;
    lookup[243] = 1;
    lookup[244] = 3;
    lookup[245] = 1;
    lookup[246] = 2;
    lookup[247] = 1;
    lookup[248] = 4;
    lookup[249] = 1;
    lookup[250] = 2;
    lookup[251] = 1;
    lookup[252] = 3;
    lookup[253] = 1;
    lookup[254] = 2;
    lookup[255] = 1;
  }
}
