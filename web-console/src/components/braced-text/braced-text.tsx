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

import classNames from 'classnames';
import { max } from 'd3-array';
import React, { Fragment } from 'react';

import './braced-text.scss';

const THOUSANDS_SEPARATOR = ','; // Maybe one day make this locale aware

export interface BracedTextProps {
  className?: string;
  text: string;
  braces: string[];
  padFractionalPart?: boolean;
  unselectableThousandsSeparator?: boolean;
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
  return longest.replace(/K/g, 'M');
}

function lengthAfterLastDot(str: string): number | undefined {
  const parts = str.split('.');
  const n = parts.length;
  if (n < 2) return;
  return parts[n - 1].length;
}

function zerosOfLength(n: number): string {
  return new Array(n + 1).join('0');
}

function arrayJoin<T, U>(array: T[], separator: U): (T | U)[] {
  const result: (T | U)[] = [];
  for (let i = 0; i < array.length; i++) {
    if (i) {
      result.push(separator, array[i]);
    } else {
      result.push(array[i]);
    }
  }
  return result;
}

function hideThousandsSeparator(text: string) {
  const parts = text.split(THOUSANDS_SEPARATOR);
  if (parts.length < 2) return text;
  return arrayJoin(parts, <span className="unselectable">{THOUSANDS_SEPARATOR}</span>).map(
    (x, i) => <Fragment key={i}>{x}</Fragment>,
  );
}

export const BracedText = React.memo(function BracedText(props: BracedTextProps) {
  const { className, text, braces, padFractionalPart, unselectableThousandsSeparator } = props;

  let effectiveBraces = braces.concat(text);

  let zeroPad: JSX.Element | undefined;
  if (padFractionalPart) {
    const lengthsAfterDot = effectiveBraces.map(lengthAfterLastDot);
    const maxLengthAfterLastDot = max(lengthsAfterDot, x => x);
    if (maxLengthAfterLastDot) {
      const textLengthAfterLastDot = lengthAfterLastDot(text);
      if (typeof textLengthAfterLastDot !== 'undefined') {
        const padLength = Math.max(maxLengthAfterLastDot - textLengthAfterLastDot, 0);
        zeroPad = <span className="zero-pad">{zerosOfLength(padLength)}</span>;
      } else {
        zeroPad = <span className="zero-pad">{`.${zerosOfLength(maxLengthAfterLastDot)}`}</span>;
      }

      effectiveBraces = effectiveBraces.map((brace, i) => {
        const braceLengthAfterLastDot = lengthsAfterDot[i];
        if (typeof braceLengthAfterLastDot !== 'undefined') {
          const padLength = Math.max(maxLengthAfterLastDot - braceLengthAfterLastDot, 0);
          return `${brace}${zerosOfLength(padLength)}`;
        } else {
          return `${brace}.${zerosOfLength(maxLengthAfterLastDot)}`;
        }
      });
    }
  }

  return (
    <span className={classNames('braced-text', className)}>
      <span className="brace-text">{findMostNumbers(effectiveBraces)}</span>
      <span className="real-text">
        {unselectableThousandsSeparator ? hideThousandsSeparator(text) : text}
        {zeroPad}
      </span>
    </span>
  );
});
