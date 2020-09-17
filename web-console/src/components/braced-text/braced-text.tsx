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

import React from 'react';

import './braced-text.scss';

export interface BracedTextProps {
  text: string;
  braces: string[];
}

export function findMostNumbers(strings: string[]): string {
  let longest = '';
  let longestNumLengthPlusOne = 1;
  for (const s of strings) {
    const parts = s.split(/\d/g);
    const numLengthPlusOne = parts.length;
    if (longestNumLengthPlusOne < numLengthPlusOne) {
      longest = parts.join('0');
      longestNumLengthPlusOne = numLengthPlusOne;
    } else if (longestNumLengthPlusOne === numLengthPlusOne && longest.length < s.length) {
      // Tie break on general length
      longest = parts.join('0');
      longestNumLengthPlusOne = numLengthPlusOne;
    }
  }
  return longest;
}

export const BracedText = React.memo(function BracedText(props: BracedTextProps) {
  const { text, braces } = props;

  return (
    <span className="braced-text">
      <span className="brace-text">{findMostNumbers(braces.concat(text))}</span>
      <span className="real-text">{text}</span>
    </span>
  );
});
