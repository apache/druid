{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
*/}}

{{ define "members" }}
    {{ range .Members }}
        {{ if not (hiddenMember .)}}
            <tr>
                <td>
                    <code>{{ fieldName . }}</code><br>
                    <em>
                        {{ if linkForType .Type }}
                            <a href="{{ linkForType .Type }}">
                                {{ typeDisplayName .Type }}
                            </a>
                        {{ else }}
                            {{ typeDisplayName .Type }}
                        {{ end }}
                    </em>
                </td>
                <td>
                    {{ if fieldEmbedded . }}
                        <p>
                            (Members of <code>{{ fieldName . }}</code> are embedded into this type.)
                        </p>
                    {{ end}}

                    {{ if isOptionalMember .}}
                        <em>(Optional)</em>
                    {{ end }}

                    {{ safe (renderComments .CommentLines) }}

                    {{ if and (eq (.Type.Name.Name) "ObjectMeta") }}
                        Refer to the Kubernetes API documentation for the fields of the
                        <code>metadata</code> field.
                    {{ end }}

                    {{ if or (eq (fieldName .) "spec") }}
                        <br/>
                        <br/>
                        <table>
                            {{ template "members" .Type }}
                        </table>
                    {{ end }}
                </td>
            </tr>
        {{ end }}
    {{ end }}
{{ end }}
