# Structured Logging

Setting `log_format: json` (or `X2S3_LOG_FORMAT=json`) makes x2s3 write one JSON object per line to stdout instead of human-readable text. The field names follow the [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html) (ECS), so Kibana's built-in visualizations understand them without any mapping work.

The default remains `text`; nothing changes unless you turn this on.

## What gets logged

Every log record carries `@timestamp`, `log.level`, `log.logger`, `message`, `service.name`, `service.version` and `process.pid`. Records emitted while serving a request also carry `trace.id`, which is the same value returned to the client in the `x-amz-request-id` response header — so a client can report an id and you can pull up every line for that request.

One access line is logged per request, tagged `event.dataset: x2s3.access`, with:

| Field | Notes |
|---|---|
| `event.duration` | Request duration in **nanoseconds** (the ECS unit). Measured until the last byte of the body is sent, so it reflects real transfer time for large objects. |
| `http.request.method`, `http.version` | |
| `http.response.status_code` | |
| `http.response.body.bytes` | Bytes actually written to the socket. |
| `url.path`, `url.query`, `url.domain` | Path is logged as the client encoded it. |
| `client.ip`, `client.port` | Corrected for the reverse proxy when running with `--proxy-headers`. |
| `user_agent.original` | Separates Neuroglancer, browser and boto traffic. |
| `labels.target` | Which configured target served the request. The dimension worth slicing latency by. |
| `error.type`, `error.message`, `error.stack_trace` | On records logged with an exception. |

## Shipping to Elasticsearch

The container logs to stdout, so a shipper reading the Docker log files needs only to parse the JSON. For Filebeat:

```yaml
filebeat.inputs:
  - type: container
    paths:
      - /var/lib/docker/containers/*/*.log
    json.keys_under_root: true
    json.add_error_key: true
```

Since ECS field names are written as dotted keys, Elasticsearch expands them into the usual nested ECS document on ingest.

## Kibana

`event.duration` is a plain number, so a percentile aggregation over it gives p95/p99 directly. Set the field's format to *Duration* (input: nanoseconds) in the data view to have Kibana render it readably. Break it down by `labels.target` to find a slow storage backend, or by `http.response.status_code` for error rates.

## Known limitation

x2s3 runs with several uvicorn workers sharing one stdout. Log lines longer than the ~4KB atomic-write limit can interleave between workers; `process.pid` is on every line so those are identifiable. If it becomes a problem, give each worker its own file sink.

Uvicorn logs two lines ("Started server process", "Waiting for application startup") before it imports the app, so those stay plain text on every start. Everything after that is JSON.
