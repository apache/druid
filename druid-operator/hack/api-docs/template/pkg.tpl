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

{{ define "packages" }}
    <h1>Druid API reference</h1>

    {{ with .packages}}
        <p>Packages:</p>
        <ul class="simple">
            {{ range . }}
                <li>
                    <a href="#{{- packageAnchorID . -}}">{{ packageDisplayName . }}</a>
                </li>
            {{ end }}
        </ul>
    {{ end}}

    {{ range .packages }}
        <h2 id="{{- packageAnchorID . -}}">
            {{- packageDisplayName . -}}
        </h2>

        {{ with (index .GoPackages 0 )}}
            {{ with .DocComments }}
                {{ safe (renderComments .) }}
            {{ end }}
        {{ end }}

        Resource Types:

        <ul class="simple">
            {{- range (visibleTypes (sortedTypes .Types)) -}}
                {{ if isExportedType . -}}
                    <li>
                        <a href="{{ linkForType . }}">{{ typeDisplayName . }}</a>
                    </li>
                {{- end }}
            {{- end -}}
        </ul>

        {{ range (visibleTypes (sortedTypes .Types))}}
            {{ template "type" .  }}
        {{ end }}
    {{ end }}

    <div class="admonition note">
        <p class="last">This page was automatically generated with <code>gen-crd-api-reference-docs</code></p>
    </div>
{{ end }}
