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

import { Button, FormGroup, Intent, TextArea } from '@blueprintjs/core';
import type { CustomFilterPattern } from '@druid-toolkit/query';
import { SqlExpression } from '@druid-toolkit/query';
import React, { useState } from 'react';

export interface CustomFilterControlProps {
  initFilterPattern: CustomFilterPattern;
  negated: boolean;
  setFilterPattern(filterPattern: CustomFilterPattern): void;
}

export const CustomFilterControl = React.memo(function CustomFilterControl(
  props: CustomFilterControlProps,
) {
  const { initFilterPattern, negated, setFilterPattern } = props;
  const [formula, setFormula] = useState<string>(String(initFilterPattern.expression || ''));

  function makePattern(): CustomFilterPattern {
    return {
      type: 'custom',
      negated,
      expression: SqlExpression.maybeParse(formula),
    };
  }

  return (
    <div className="custom-filter-control">
      <FormGroup>
        <TextArea
          value={formula}
          onChange={e => setFormula(e.target.value)}
          placeholder="SQL expression"
        />
      </FormGroup>
      <div className="button-bar">
        <Button
          intent={Intent.PRIMARY}
          text="OK"
          onClick={() => {
            const newPattern = makePattern();
            // TODO check if valid
            // if (!isFilterPatternValid(newPattern)) return;
            setFilterPattern(newPattern);
          }}
        />
      </div>
    </div>
  );
});
