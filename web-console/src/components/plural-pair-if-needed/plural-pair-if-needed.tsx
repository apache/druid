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

import { compact, pluralIfNeeded } from '../../utils';

export interface PluralPairIfNeededProps {
  firstCount: number;
  firstSingular: string;
  secondCount: number;
  secondSingular: string;
}

export const PluralPairIfNeeded = React.memo(function PluralPairIfNeeded(
  props: PluralPairIfNeededProps,
) {
  const { firstCount, firstSingular, secondCount, secondSingular } = props;

  const text = compact([
    firstCount ? pluralIfNeeded(firstCount, firstSingular) : undefined,
    secondCount ? pluralIfNeeded(secondCount, secondSingular) : undefined,
  ]).join(', ');
  if (!text) return null;
  return <p className="plural-pair-if-needed">{text}</p>;
});
