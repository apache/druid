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

import type { JSX } from 'react';
import React from 'react';

import './highlight-text.scss';

export interface HighlightTextProps {
  text: string;
  find: string | RegExp;
  replace: string | JSX.Element | ((found: string) => string | JSX.Element);
}

export const HighlightText = React.memo(function HighlightText(props: HighlightTextProps) {
  const { text, find, replace } = props;

  let startIndex = -1;
  let found = '';

  if (typeof find === 'string') {
    startIndex = text.indexOf(find);
    if (startIndex !== -1) {
      found = find;
    }
  } else {
    const m = find.exec(text);
    if (m) {
      startIndex = m.index;
      found = m[0];
    }
  }

  if (startIndex === -1) return <span className="highlight-text">text</span>;
  const endIndex = startIndex + found.length;

  const pre = text.substring(0, startIndex);
  const post = text.substring(endIndex);
  const replaceValue = typeof replace === 'function' ? replace(found) : replace;
  return (
    <span className="highlight-text">
      {Boolean(pre) && <span className="pre">{text.substring(0, startIndex)}</span>}
      {typeof replaceValue === 'string' ? (
        <span className="highlighted">{replaceValue}</span>
      ) : (
        replaceValue
      )}
      {Boolean(post) && <span className="post">{text.substring(endIndex)}</span>}
    </span>
  );
});
