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

import React, { useState } from 'react';

import { DruidError, RowColumn } from '../../utils';
import { HighlightText } from '../highlight-text/highlight-text';

import './query-error-pane.scss';

export interface QueryErrorPaneProps {
  error: DruidError;
  moveCursorTo: (rowColumn: RowColumn) => void;
  queryString?: string;
  onQueryStringChange?: (newQueryString: string, run?: boolean) => void;
}

export const QueryErrorPane = React.memo(function QueryErrorPane(props: QueryErrorPaneProps) {
  const { error, moveCursorTo, queryString, onQueryStringChange } = props;
  const [showMode, setShowMore] = useState(false);

  if (!error.errorMessage) {
    return <div className="query-error-pane">{error.message}</div>;
  }

  const { position, suggestion } = error;
  let suggestionElement: JSX.Element | undefined;
  if (suggestion && queryString && onQueryStringChange) {
    const newQuery = suggestion.fn(queryString);
    if (newQuery) {
      suggestionElement = (
        <p>
          Suggestion:{' '}
          <a
            onClick={() => {
              onQueryStringChange(newQuery, true);
            }}
          >
            {suggestion.label}
          </a>
        </p>
      );
    }
  }

  return (
    <div className="query-error-pane">
      {suggestionElement}
      {error.error && <p>{`Error: ${error.error}`}</p>}
      {error.errorMessageWithoutExpectation && (
        <p>
          {position ? (
            <HighlightText
              text={error.errorMessageWithoutExpectation}
              find={position.match}
              replace={
                <a
                  onClick={() => {
                    moveCursorTo(position);
                  }}
                >
                  {position.match}
                </a>
              }
            />
          ) : (
            error.errorMessageWithoutExpectation
          )}
          {error.expectation && !showMode && (
            <>
              {' '}
              <a onClick={() => setShowMore(true)}>More...</a>
            </>
          )}
        </p>
      )}
      {error.expectation && showMode && (
        <p>
          {error.expectation} <a onClick={() => setShowMore(false)}>Less...</a>
        </p>
      )}
      {error.errorClass && <p>{error.errorClass}</p>}
    </div>
  );
});
