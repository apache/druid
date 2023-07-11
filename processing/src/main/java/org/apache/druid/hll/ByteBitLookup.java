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

package org.apache.druid.hll;

/**
 */
public class ByteBitLookup
{
  public static final byte[] LOOKUP;

  static {
    LOOKUP = new byte[256];

    LOOKUP[0] = 0;
    LOOKUP[1] = 1;
    LOOKUP[2] = 2;
    LOOKUP[3] = 1;
    LOOKUP[4] = 3;
    LOOKUP[5] = 1;
    LOOKUP[6] = 2;
    LOOKUP[7] = 1;
    LOOKUP[8] = 4;
    LOOKUP[9] = 1;
    LOOKUP[10] = 2;
    LOOKUP[11] = 1;
    LOOKUP[12] = 3;
    LOOKUP[13] = 1;
    LOOKUP[14] = 2;
    LOOKUP[15] = 1;
    LOOKUP[16] = 5;
    LOOKUP[17] = 1;
    LOOKUP[18] = 2;
    LOOKUP[19] = 1;
    LOOKUP[20] = 3;
    LOOKUP[21] = 1;
    LOOKUP[22] = 2;
    LOOKUP[23] = 1;
    LOOKUP[24] = 4;
    LOOKUP[25] = 1;
    LOOKUP[26] = 2;
    LOOKUP[27] = 1;
    LOOKUP[28] = 3;
    LOOKUP[29] = 1;
    LOOKUP[30] = 2;
    LOOKUP[31] = 1;
    LOOKUP[32] = 6;
    LOOKUP[33] = 1;
    LOOKUP[34] = 2;
    LOOKUP[35] = 1;
    LOOKUP[36] = 3;
    LOOKUP[37] = 1;
    LOOKUP[38] = 2;
    LOOKUP[39] = 1;
    LOOKUP[40] = 4;
    LOOKUP[41] = 1;
    LOOKUP[42] = 2;
    LOOKUP[43] = 1;
    LOOKUP[44] = 3;
    LOOKUP[45] = 1;
    LOOKUP[46] = 2;
    LOOKUP[47] = 1;
    LOOKUP[48] = 5;
    LOOKUP[49] = 1;
    LOOKUP[50] = 2;
    LOOKUP[51] = 1;
    LOOKUP[52] = 3;
    LOOKUP[53] = 1;
    LOOKUP[54] = 2;
    LOOKUP[55] = 1;
    LOOKUP[56] = 4;
    LOOKUP[57] = 1;
    LOOKUP[58] = 2;
    LOOKUP[59] = 1;
    LOOKUP[60] = 3;
    LOOKUP[61] = 1;
    LOOKUP[62] = 2;
    LOOKUP[63] = 1;
    LOOKUP[64] = 7;
    LOOKUP[65] = 1;
    LOOKUP[66] = 2;
    LOOKUP[67] = 1;
    LOOKUP[68] = 3;
    LOOKUP[69] = 1;
    LOOKUP[70] = 2;
    LOOKUP[71] = 1;
    LOOKUP[72] = 4;
    LOOKUP[73] = 1;
    LOOKUP[74] = 2;
    LOOKUP[75] = 1;
    LOOKUP[76] = 3;
    LOOKUP[77] = 1;
    LOOKUP[78] = 2;
    LOOKUP[79] = 1;
    LOOKUP[80] = 5;
    LOOKUP[81] = 1;
    LOOKUP[82] = 2;
    LOOKUP[83] = 1;
    LOOKUP[84] = 3;
    LOOKUP[85] = 1;
    LOOKUP[86] = 2;
    LOOKUP[87] = 1;
    LOOKUP[88] = 4;
    LOOKUP[89] = 1;
    LOOKUP[90] = 2;
    LOOKUP[91] = 1;
    LOOKUP[92] = 3;
    LOOKUP[93] = 1;
    LOOKUP[94] = 2;
    LOOKUP[95] = 1;
    LOOKUP[96] = 6;
    LOOKUP[97] = 1;
    LOOKUP[98] = 2;
    LOOKUP[99] = 1;
    LOOKUP[100] = 3;
    LOOKUP[101] = 1;
    LOOKUP[102] = 2;
    LOOKUP[103] = 1;
    LOOKUP[104] = 4;
    LOOKUP[105] = 1;
    LOOKUP[106] = 2;
    LOOKUP[107] = 1;
    LOOKUP[108] = 3;
    LOOKUP[109] = 1;
    LOOKUP[110] = 2;
    LOOKUP[111] = 1;
    LOOKUP[112] = 5;
    LOOKUP[113] = 1;
    LOOKUP[114] = 2;
    LOOKUP[115] = 1;
    LOOKUP[116] = 3;
    LOOKUP[117] = 1;
    LOOKUP[118] = 2;
    LOOKUP[119] = 1;
    LOOKUP[120] = 4;
    LOOKUP[121] = 1;
    LOOKUP[122] = 2;
    LOOKUP[123] = 1;
    LOOKUP[124] = 3;
    LOOKUP[125] = 1;
    LOOKUP[126] = 2;
    LOOKUP[127] = 1;
    LOOKUP[128] = 8;
    LOOKUP[129] = 1;
    LOOKUP[130] = 2;
    LOOKUP[131] = 1;
    LOOKUP[132] = 3;
    LOOKUP[133] = 1;
    LOOKUP[134] = 2;
    LOOKUP[135] = 1;
    LOOKUP[136] = 4;
    LOOKUP[137] = 1;
    LOOKUP[138] = 2;
    LOOKUP[139] = 1;
    LOOKUP[140] = 3;
    LOOKUP[141] = 1;
    LOOKUP[142] = 2;
    LOOKUP[143] = 1;
    LOOKUP[144] = 5;
    LOOKUP[145] = 1;
    LOOKUP[146] = 2;
    LOOKUP[147] = 1;
    LOOKUP[148] = 3;
    LOOKUP[149] = 1;
    LOOKUP[150] = 2;
    LOOKUP[151] = 1;
    LOOKUP[152] = 4;
    LOOKUP[153] = 1;
    LOOKUP[154] = 2;
    LOOKUP[155] = 1;
    LOOKUP[156] = 3;
    LOOKUP[157] = 1;
    LOOKUP[158] = 2;
    LOOKUP[159] = 1;
    LOOKUP[160] = 6;
    LOOKUP[161] = 1;
    LOOKUP[162] = 2;
    LOOKUP[163] = 1;
    LOOKUP[164] = 3;
    LOOKUP[165] = 1;
    LOOKUP[166] = 2;
    LOOKUP[167] = 1;
    LOOKUP[168] = 4;
    LOOKUP[169] = 1;
    LOOKUP[170] = 2;
    LOOKUP[171] = 1;
    LOOKUP[172] = 3;
    LOOKUP[173] = 1;
    LOOKUP[174] = 2;
    LOOKUP[175] = 1;
    LOOKUP[176] = 5;
    LOOKUP[177] = 1;
    LOOKUP[178] = 2;
    LOOKUP[179] = 1;
    LOOKUP[180] = 3;
    LOOKUP[181] = 1;
    LOOKUP[182] = 2;
    LOOKUP[183] = 1;
    LOOKUP[184] = 4;
    LOOKUP[185] = 1;
    LOOKUP[186] = 2;
    LOOKUP[187] = 1;
    LOOKUP[188] = 3;
    LOOKUP[189] = 1;
    LOOKUP[190] = 2;
    LOOKUP[191] = 1;
    LOOKUP[192] = 7;
    LOOKUP[193] = 1;
    LOOKUP[194] = 2;
    LOOKUP[195] = 1;
    LOOKUP[196] = 3;
    LOOKUP[197] = 1;
    LOOKUP[198] = 2;
    LOOKUP[199] = 1;
    LOOKUP[200] = 4;
    LOOKUP[201] = 1;
    LOOKUP[202] = 2;
    LOOKUP[203] = 1;
    LOOKUP[204] = 3;
    LOOKUP[205] = 1;
    LOOKUP[206] = 2;
    LOOKUP[207] = 1;
    LOOKUP[208] = 5;
    LOOKUP[209] = 1;
    LOOKUP[210] = 2;
    LOOKUP[211] = 1;
    LOOKUP[212] = 3;
    LOOKUP[213] = 1;
    LOOKUP[214] = 2;
    LOOKUP[215] = 1;
    LOOKUP[216] = 4;
    LOOKUP[217] = 1;
    LOOKUP[218] = 2;
    LOOKUP[219] = 1;
    LOOKUP[220] = 3;
    LOOKUP[221] = 1;
    LOOKUP[222] = 2;
    LOOKUP[223] = 1;
    LOOKUP[224] = 6;
    LOOKUP[225] = 1;
    LOOKUP[226] = 2;
    LOOKUP[227] = 1;
    LOOKUP[228] = 3;
    LOOKUP[229] = 1;
    LOOKUP[230] = 2;
    LOOKUP[231] = 1;
    LOOKUP[232] = 4;
    LOOKUP[233] = 1;
    LOOKUP[234] = 2;
    LOOKUP[235] = 1;
    LOOKUP[236] = 3;
    LOOKUP[237] = 1;
    LOOKUP[238] = 2;
    LOOKUP[239] = 1;
    LOOKUP[240] = 5;
    LOOKUP[241] = 1;
    LOOKUP[242] = 2;
    LOOKUP[243] = 1;
    LOOKUP[244] = 3;
    LOOKUP[245] = 1;
    LOOKUP[246] = 2;
    LOOKUP[247] = 1;
    LOOKUP[248] = 4;
    LOOKUP[249] = 1;
    LOOKUP[250] = 2;
    LOOKUP[251] = 1;
    LOOKUP[252] = 3;
    LOOKUP[253] = 1;
    LOOKUP[254] = 2;
    LOOKUP[255] = 1;
  }
}
