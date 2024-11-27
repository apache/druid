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

import type { IconName } from '@blueprintjs/icons';
import type { CancelToken } from 'axios';
import type { QueryResult, SqlExpression, SqlQuery } from 'druid-query-toolkit';

import type { Stage } from '../../../utils/stage';
import type { ParameterDefinition, QuerySource } from '../models';

interface ModuleDefinition<P> {
  id: string;
  icon: IconName;
  title: string;
  parameters: Record<keyof P, ParameterDefinition>;
  component: (props: ModuleComponentProps<P>) => any;
}

interface ModuleComponentProps<P> {
  stage: Stage;
  querySource: QuerySource;
  where: SqlExpression;
  setWhere(where: SqlExpression): void;
  parameterValues: P;
  setParameterValues: (parameters: Partial<P>) => void;
  runSqlQuery(query: string | SqlQuery, cancelToken?: CancelToken): Promise<QueryResult>;
}

export class ModuleRepository {
  private static readonly repo = new Map<string, ModuleDefinition<any>>();

  static registerModule<P>(module: ModuleDefinition<P>) {
    if (ModuleRepository.repo.has(module.id)) {
      throw new Error(`duplicate module definition for id: ${module.id}`);
    }
    ModuleRepository.repo.set(module.id, module);
  }

  static getModule(id: string) {
    return ModuleRepository.repo.get(id);
  }

  static getAllModuleEntries(): ModuleDefinition<any>[] {
    return [...ModuleRepository.repo.values()];
  }
}
