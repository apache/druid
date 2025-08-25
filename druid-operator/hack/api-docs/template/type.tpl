{{ define "type" }}
    <h3 id="{{ anchorIDForType . }}">
        {{- .Name.Name }}
        {{ if eq .Kind "Alias" }}(<code>{{.Underlying}}</code> alias){{ end -}}
    </h3>

    {{ with (typeReferences .) }}
        <p>
            (<em>Appears on:</em>
            {{- $prev := "" -}}
            {{- range . -}}
                {{- if $prev -}}, {{ end -}}
                {{ $prev = . }}
                <a href="{{ linkForType . }}">{{ typeDisplayName . }}</a>
            {{- end -}}
            )
        </p>
    {{ end }}

    {{ with .CommentLines }}
        {{ safe (renderComments .) }}
    {{ end }}

    {{ if .Members }}
        <div class="md-typeset__scrollwrap">
            <div class="md-typeset__table">
                <table>
                    <thead>
                    <tr>
                        <th>Field</th>
                        <th>Description</th>
                    </tr>
                    </thead>
                    <tbody>
                    {{ if isExportedType . }}
                        <tr>
                            <td>
                                <code>apiVersion</code><br>
                                string</td>
                            <td>
                                <code>{{ apiGroup . }}</code>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <code>kind</code><br>
                                string
                            </td>
                            <td>
                                <code>{{ .Name.Name }}</code>
                            </td>
                        </tr>
                    {{ end }}
                    {{ template "members" . }}
                    </tbody>
                </table>
            </div>
        </div>
    {{ end }}
{{ end }}
