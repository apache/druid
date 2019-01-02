# druid-watermarks-console

_Note: This tool is very much a work in progress... the code might be a bit.. rough around the edges_

A standalone web ui for viewing status, monitoring, and management of the 
Druid watermarks system. 

![screenshot](TODO)

The watermark console is a static html/js web application built
with [react](https://facebook.github.io/react/) that interacts with the API of the 
`watermark-collector` service, intended to be deployed and hosted from a cloud storage 
bucket such as AWS S3 or Google Cloud Storage. 

## Development

To build `npm install && npm run build`. The file `public/index.html` can then be loaded
in a browser window, and will make cross origin requests to the watermark collector.

`npm run watch` to auto rebuild during development.

## Docker

This web app is currently designed to be run on k8s from a Docker container.
The current setup launches an nginx configuration that allows targeting other k8s containers (the watermark keeper hostnames).

In that scenario the way to access the console is with the appropriate `kubectl port-forward` command and then going to `http://localhost:8080/console/`

An example using gke is below:

```
$ gcloud container clusters get-credentials some-k8s-cluster --zone some_zone --project some_project && kubectl port-forward $(kubectl get pod --selector="some_redacted_selector" --output jsonpath='{.items[0].metadata.name}') 8080:8080
Fetching cluster endpoint and auth data.
kubeconfig entry generated for some-k8s-cluster.
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

There are two important environment variables for the container

|Env Var| Description|
|-------|------------|
|`WATERMARK_API_HOST`|The watermark keeper host|
|`WATERMARK_API_PORT`|The watermark keeper port|

These work well if a watermark keeper is deployed in the same k8s cluster and the variables are for the keeper's k8s Service.
