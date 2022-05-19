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

import './highlight-text.scss';

export interface HighlightTextProps {
  text: string;
  find: string;
  replace: string | JSX.Element;
}

export const HighlightText = React.memo(function HighlightText(props: HighlightTextProps) {
  const { text, find, replace } = props;

  const startIndex = text.indexOf(find);
  if (startIndex === -1) return <span className="highlight-text">text</span>;
  const endIndex = startIndex + find.length;

  const pre = text.substring(0, startIndex);
  const post = text.substring(endIndex);
  return (
    <span className="highlight-text">
      {Boolean(pre) && <span className="pre">{text.substring(0, startIndex)}</span>}
      {typeof replace === 'string' ? <span className="highlighted">{replace}</span> : replace}
      {Boolean(post) && <span className="post">{text.substring(endIndex)}</span>}
    </span>
  );
});
