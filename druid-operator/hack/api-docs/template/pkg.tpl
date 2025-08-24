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
