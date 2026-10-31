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

import type React from 'react';
import { createContext, useContext } from 'react';

import type { QueryContextParameterMetadata } from '../druid-models';

const QueryContextParametersContext = createContext<
  readonly QueryContextParameterMetadata[] | undefined
>(undefined);

export interface QueryContextParametersProviderProps {
  queryContextParameters?: readonly QueryContextParameterMetadata[];
  children: React.ReactNode;
}

export const QueryContextParametersProvider: React.FC<QueryContextParametersProviderProps> = ({
  queryContextParameters,
  children,
}) => {
  return (
    <QueryContextParametersContext value={queryContextParameters}>
      {children}
    </QueryContextParametersContext>
  );
};

/**
 * The query context parameters from sys.query_context_parameters, or undefined when they could not be loaded (an
 * older Druid or no access to sys tables)
 */
export const useQueryContextParameters = () => {
  return useContext(QueryContextParametersContext);
};
