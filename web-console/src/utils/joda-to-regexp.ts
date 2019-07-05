/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Refer to https://www.joda.org/joda-time/key_format.html
const TEXT = '\\w+';
const NUMBER_2_DIGIT = '[0-9]{2}';
const NUMBER_4_DIGIT = '[0-9]{4}';
const JODA_FRAGMENT_TO_REG_EXP_STRING: Record<string, string> = {
  C: '[0-9]{1,2}',
  CC: NUMBER_2_DIGIT,
  YY: NUMBER_2_DIGIT,
  YYYY: NUMBER_4_DIGIT,

  xx: NUMBER_2_DIGIT,
  xxxx: NUMBER_4_DIGIT,
  w: '[0-9]{1,2}',
  ww: NUMBER_2_DIGIT,
  e: '[0-7]',
  E: TEXT,
  EEEE: TEXT,

  yy: NUMBER_2_DIGIT,
  yyyy: NUMBER_4_DIGIT,
  D: '[0-9]{1,3}',
  DD: '[0-9]{2,3}',
  DDD: '[0-9]{3}',
  M: '(?:1[0-2]|[1-9])',
  MM: '(?:1[0-2]|0[1-9])',
  MMM: TEXT,
  MMMM: TEXT,
  d: '(?:3[0-1]|[12][0-9]|[1-9])',
  dd: '(?:3[0-1]|[12][0-9]|0[1-9])',

  a: '[ap]m',
  K: '(?:1[01]|[0-9])',
  KK: '(?:1[01]|0[0-9])',
  h: '(?:1[0-2]|[1-9])',
  hh: '(?:1[0-2]|0[1-9])',

  H: '(?:2[0-3]|1[0-9]|[0-9])',
  HH: '(?:2[0-3]|1[0-9]|0[0-9])',
  k: '(?:2[0-4]|1[0-9]|[1-9])',
  kk: '(?:2[0-4]|1[0-9]|0[1-9])',
  m: '(?:[1-5][0-9]|[0-9])',
  mm: '[0-5][0-9]',
  s: '(?:[1-5][0-9]|[0-9])',
  ss: '[0-5][0-9]',
  S: '[0-9]{1,3}',
  SS: '[0-9]{2,3}',
  SSS: '[0-9]{3}',
  z: TEXT,
  Z: TEXT,
};

export function jodaFormatToRegExp(jodaFormat: string): RegExp {
  const regExpStr = jodaFormat.replace(/([a-zA-Z])\1{0,3}/g, jodaPart => {
    const re = JODA_FRAGMENT_TO_REG_EXP_STRING[jodaPart];
    if (!re) throw new Error(`could not convert ${jodaPart} to RegExp`);
    return re;
  });
  return new RegExp(`^${regExpStr}$`, 'i');
}
