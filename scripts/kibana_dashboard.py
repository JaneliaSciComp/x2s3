#!/usr/bin/env python3
"""Probe the x2s3 log index in Kibana and (re)build the production dashboard.

Credentials come from the environment or var/kibana.env, which is where this
project already keeps secrets and is gitignored. Not the repo-root .env: pydantic
reads that one into Settings, which forbids unknown keys and would refuse to
start the app. Either an API key:

    KIBANA_API_KEY=<base64 of id:secret, as shown by Stack Management > API keys>

a session cookie copied from the browser (works when Kibana is behind SSO):

    KIBANA_COOKIE=<value of the 'sid' cookie>

or a native-realm username and password:

    KIBANA_USER=you
    KIBANA_PASS=...

Usage:
    scripts/kibana_dashboard.py probe     # what's actually in the index
    scripts/kibana_dashboard.py push      # create/overwrite the dashboard
    scripts/kibana_dashboard.py dump      # print the NDJSON, import nothing
"""
import base64
import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("KIBANA_URL", "https://kibana-ocp.int.janelia.org").rstrip("/")
SPACE = os.environ.get("KIBANA_SPACE", "s3proxy")
DV_TITLE = os.environ.get("KIBANA_DATA_VIEW", "filebeat-s3proxy-*")

# Logical name -> field in the index. Defaults are the ECS names x2s3/log.py
# writes; probe reports what the shipper actually indexed, and a shipper that
# nests or keyword-suffixes them only needs fixing here.
F = {
    "status": "http.response.status_code",
    "duration": "event.duration_ms",
    "bytes": "http.response.body.bytes",
    "method": "http.request.method",
    "target": "labels.target",
    "path": "url.path",
    "client": "client.ip",
    "agent": "user_agent.original",
    "level": "log.level",
    "trace": "trace.id",
    "host": "host.name",
    "referrer": "http.request.referrer",
}

# Client classification. Matched against the analyzed .text subfield so the terms
# are case-insensitive and match anywhere in the UA string.
#
# These two groups are the ones a user agent can actually identify. The bulk of
# the non-human traffic is a distributed scraper that sends byte-identical real
# Chrome strings from thousands of addresses, so no UA rule can catch it; the
# "user agents by unique clients" panel is what exposes it, and blocking it needs
# a network-level rule x2s3's log has no field for.
MONITORING = ["bot", "crawler", "spider", "uptime", "monitoring",
              "gatus", "pingdom", "zabbix", "nagios", "healthcheck"]
TOOLS = ["aws", "boto", "botocore", "ahc", "curl", "wget", "okhttp", "aiohttp",
         "python", "java", "s5cmd", "rclone", "minio", "go-http-client"]
# Every access-log line has a status code; startup and stream-progress lines do
# not. Filtering on it keeps counts honest without depending on event.dataset
# surviving the shipper.
DASHBOARD_QUERY = f"{F['status']}: *"


def load_dotenv(path):
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def auth_headers():
    """Whichever of the three credential styles var/kibana.env supplies.

    An SSO-backed stack has no native-realm password to send, so an API key or a
    copied browser session cookie are the only ones that authenticate there.
    """
    if os.environ.get("KIBANA_API_KEY"):
        return {"Authorization": f"ApiKey {os.environ['KIBANA_API_KEY']}"}
    if os.environ.get("KIBANA_COOKIE"):
        cookie = os.environ["KIBANA_COOKIE"]
        return {"Cookie": cookie if "=" in cookie else f"sid={cookie}"}
    user, password = os.environ.get("KIBANA_USER"), os.environ.get("KIBANA_PASS")
    if not (user and password):
        sys.exit("Set KIBANA_API_KEY, KIBANA_COOKIE, or KIBANA_USER/KIBANA_PASS "
                 "in var/kibana.env")
    return {"Authorization": "Basic "
            + base64.b64encode(f"{user}:{password}".encode()).decode()}


def call(path, data=None, headers=None, method=None, missing_ok=False):
    url = f"{BASE}/s/{SPACE}{path}"
    request = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"))
    for key, value in auth_headers().items():
        request.add_header(key, value)
    request.add_header("kbn-xsrf", "true")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if missing_ok and error.code == 404:
            return None
        body = error.read().decode(errors="replace")[:2000]
        sys.exit(f"{error.code} {error.reason} from {path}\n{body}")


def es(path, query=None):
    """Run an Elasticsearch request through Kibana's console proxy."""
    # The proxy route is itself POST-only; the ES verb goes in &method=.
    return call(
        f"/api/console/proxy?path={urllib.request.quote(path, safe='/*_,')}"
        f"&method={'POST' if query else 'GET'}",
        data=json.dumps(query).encode() if query is not None else b"",
        headers={"Content-Type": "application/json"},
        method="POST",
    )


def kibana_major():
    if os.environ.get("KIBANA_VERSION"):
        return int(os.environ["KIBANA_VERSION"].split(".")[0])
    # This deployment's /api/status omits the version block, so assume the
    # current major and let KIBANA_VERSION override if the import complains.
    status = call("/api/status")
    return int(status.get("version", {}).get("number", "8.0.0").split(".")[0])


def data_view_id():
    for view in call("/api/data_views")["data_view"]:
        if view["title"] == DV_TITLE:
            return view["id"]
    sys.exit(f"No data view titled {DV_TITLE!r}; found: "
             + ", ".join(v["title"] for v in call("/api/data_views")["data_view"]))


# --------------------------------------------------------------------------- probe

def probe():
    print(f"kibana major: {kibana_major()}")
    print(f"data view   : {DV_TITLE} -> {data_view_id()}")

    caps = es(f"{DV_TITLE}/_field_caps?fields=*")["fields"]
    print("\nfields (expected -> indexed type):")
    for name, field in sorted(F.items()):
        types = sorted(caps.get(field, {}))
        keyword = f"{field}.keyword" in caps
        note = "  (.keyword exists)" if keyword else ""
        print(f"  {name:9} {field:32} {','.join(types) or 'MISSING'}{note}")
    extra = [f for f in caps if f.startswith(("labels.", "service.", "event.", "http.", "url."))]
    print("\nother log fields present: " + ", ".join(sorted(extra)[:40]))

    result = es(f"{DV_TITLE}/_search", {
        "size": 1,
        "sort": [{"@timestamp": "desc"}],
        "query": {"bool": {"filter": [
            {"exists": {"field": F["status"]}},
            {"range": {"@timestamp": {"gte": "now-24h"}}},
        ]}},
        "aggs": {
            "status": {"terms": {"field": F["status"], "size": 10}},
            "targets": {"terms": {"field": F["target"], "size": 10}},
            "levels": {"terms": {"field": F["level"], "size": 5}},
            "p95": {"percentiles": {"field": F["duration"], "percents": [50, 95, 99]}},
            "bytes": {"sum": {"field": F["bytes"]}},
        },
    })
    total = result["hits"]["total"]
    print(f"\naccess lines in last 24h: {total.get('value')} ({total.get('relation')})")
    hits = result["hits"]["hits"]
    if hits:
        print("newest document:\n" + json.dumps(hits[0]["_source"], indent=2)[:2500])
    for name in ("status", "targets", "levels"):
        buckets = result["aggregations"][name]["buckets"]
        print(f"{name}: " + ", ".join(f"{b['key']}={b['doc_count']}" for b in buckets))
    print("latency ms: " + json.dumps(result["aggregations"]["p95"]["values"]))
    print(f"bytes served 24h: {result['aggregations']['bytes']['value']:,.0f}")


# ------------------------------------------------------------------- dashboard build

def columns(major):
    records = "___records___" if major >= 8 else "Records"

    def count(label="Requests"):
        return {"label": label, "dataType": "number", "operationType": "count",
                "isBucketed": False, "scale": "ratio", "sourceField": records}

    def count_where(label, query):
        column = count(label)
        column["filter"] = {"query": query, "language": "kuery"}
        return column

    def unique(field, label):
        return {"label": label, "dataType": "number", "operationType": "unique_count",
                "sourceField": field, "isBucketed": False, "scale": "ratio"}

    def percentile(p, label=None):
        return {"label": label or f"p{p} latency", "dataType": "number",
                "operationType": "percentile", "sourceField": F["duration"],
                "isBucketed": False, "scale": "ratio",
                "params": {"percentile": p, "format": {"id": "number", "params": {
                    "decimals": 1, "suffix": " ms"}}}}

    def total_bytes(label="Bytes served"):
        return {"label": label, "dataType": "number", "operationType": "sum",
                "sourceField": F["bytes"], "isBucketed": False, "scale": "ratio",
                "params": {"format": {"id": "bytes", "params": {"decimals": 1}}}}

    def date_histogram():
        return {"label": "@timestamp", "dataType": "date", "operationType": "date_histogram",
                "sourceField": "@timestamp", "isBucketed": True, "scale": "interval",
                "params": {"interval": "auto", "includeEmptyRows": True, "dropPartials": False}}

    def terms(field, size, order_by, label):
        return {"label": label, "dataType": "string", "operationType": "terms",
                "sourceField": field, "isBucketed": True, "scale": "ordinal",
                "params": {"size": size, "orderBy": {"type": "column", "columnId": order_by},
                           "orderDirection": "desc", "otherBucket": False,
                           "missingBucket": False, "parentFormat": {"id": "terms"}}}

    def client_classes():
        ua = f"{F['agent']}.text"
        mon = f"{ua}: ({' or '.join(MONITORING)})"
        tool = f"{ua}: ({' or '.join(TOOLS)})"
        browser = f"{ua}: mozilla"
        referred = f"{F['referrer']}: *"
        # Mutually exclusive, so the stack sums to total traffic.
        rest = f"not {mon} and not {tool}"
        classes = [
            ("Monitoring", mon),
            ("SDK / tool", f"not {mon} and {tool}"),
            ("Browser, referred", f"{rest} and {browser} and {referred}"),
            ("Browser, no referrer", f"{rest} and {browser} and not {referred}"),
            ("Other", f"{rest} and not {browser}"),
        ]
        return {"label": "Client type", "dataType": "string", "operationType": "filters",
                "isBucketed": True, "scale": "ordinal",
                "params": {"filters": [{"label": label,
                                        "input": {"language": "kuery", "query": query}}
                                       for label, query in classes]}}

    def status_classes():
        ranges = [("2xx success", 200, 300), ("3xx redirect", 300, 400),
                  ("4xx client error", 400, 500), ("5xx server error", 500, 600)]
        return {"label": "Status", "dataType": "string", "operationType": "filters",
                "isBucketed": True, "scale": "ordinal",
                "params": {"filters": [
                    {"label": label, "input": {"language": "kuery", "query":
                     f"{F['status']} >= {low} and {F['status']} < {high}"}}
                    for label, low, high in ranges]}}

    return (count, count_where, unique, percentile, total_bytes, date_histogram,
            terms, status_classes, client_classes)


def lens(obj_id, title, vis_type, visualization, cols, order, dv, major, query=""):
    ds_key = "formBased" if major >= 8 else "indexpattern"
    refs = [{"type": "index-pattern", "id": dv, "name": "indexpattern-datasource-layer-l1"}]
    if major < 8:
        refs.append({"type": "index-pattern", "id": dv,
                     "name": "indexpattern-datasource-current-indexpattern"})
    return {"id": obj_id, "type": "lens", "references": refs, "attributes": {
        "title": title, "description": "", "visualizationType": vis_type,
        "state": {
            "visualization": visualization,
            "query": {"query": query, "language": "kuery"},
            "filters": [],
            "datasourceStates": {ds_key: {"layers": {"l1": {
                "columnOrder": order, "columns": cols, "incompleteColumns": {}}}}},
        }}}


def xy(series_type, x, y, split=None, legend="bottom"):
    """legend=None for a single-series chart the title already names.

    Legends default to the bottom edge rather than the right. A right-hand legend
    is laid out inside the panel and takes 130-300px off the plot, so two stacked
    charts whose legends differ in width get plot areas of different widths and
    their time axes no longer line up. A bottom legend costs height instead, which
    leaves every plot spanning the full panel width and the x-axes aligned.
    """
    layer = {"layerId": "l1", "layerType": "data", "seriesType": series_type,
             "xAccessor": x, "accessors": y}
    if split:
        layer["splitAccessor"] = split
    # 'position' only accepts an edge; hiding a legend is isVisible, not a position.
    return {"preferredSeriesType": series_type,
            "legend": {"isVisible": legend is not None, "position": legend or "right"},
            "valueLabels": "hide", "fittingFunction": "None",
            "axisTitlesVisibilitySettings": {"x": False, "yLeft": False, "yRight": False},
            "gridlinesVisibilitySettings": {"x": False, "yLeft": True, "yRight": False},
            "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": False},
            "layers": [layer]}


def metric_vis(accessor, major):
    if major >= 8:
        return {"layerId": "l1", "layerType": "data", "metricAccessor": accessor}
    return {"layerId": "l1", "layerType": "data", "accessor": accessor}


def exclude_filter(alias, terms, dv, index):
    """A negated, switched-off filter pill matching any of `terms` in the UA.

    Shipped disabled so the panels still show the full picture; enabling one is a
    single click in the filter bar, which is the only honest way to offer this
    given that the terms are a heuristic.
    """
    return {
        "meta": {"alias": alias, "negate": True, "disabled": True, "type": "custom",
                 "key": "query", "index": dv},
        "query": {"bool": {"minimum_should_match": 1, "should": [
            {"match": {f"{F['agent']}.text": term}} for term in terms]}},
        "$state": {"store": "appState"},
    }


def build(dv, major):
    (count, count_where, unique, percentile, total_bytes, date_histogram,
     terms, status_classes, client_classes) = columns(major)
    objects = []

    def add(obj_id, title, vis_type, vis, cols, order, grid, query=""):
        objects.append((lens(obj_id, title, vis_type, vis, cols, order, dv, major, query), grid))

    # Grid slots are (x, y, w, h) on Kibana's 48-column grid, and mirror the
    # arrangement as hand-tuned in the UI: a 3x2 block of headline numbers, then
    # the four comparable full-width time series stacked so their x-axes line up,
    # then each remaining series beside the table that breaks it down.

    # Volume, row 1.
    add("x2s3-requests", "Requests", "lnsMetric", metric_vis("m", major),
        {"m": count()}, ["m"], (0, 0, 10, 7))
    add("x2s3-bytes", "Bytes served", "lnsMetric", metric_vis("m", major),
        {"m": total_bytes()}, ["m"], (10, 0, 10, 7))
    add("x2s3-unique-clients", "Unique clients", "lnsMetric", metric_vis("m", major),
        {"m": unique(F["client"], "Unique IPs")}, ["m"], (20, 0, 10, 7))

    add("x2s3-top-targets", "Top targets", "lnsXY",
        xy("bar_horizontal", "b", ["m"], legend=None),
        {"b": terms(F["target"], 10, "m", "Target"), "m": count()}, ["b", "m"],
        (30, 0, 18, 14))

    # Health, row 2, sitting under the volume number each one qualifies.
    add("x2s3-p95", "p95 latency", "lnsMetric", metric_vis("m", major),
        {"m": percentile(95)}, ["m"], (0, 7, 10, 7))
    add("x2s3-server-errors", "Server errors (5xx)", "lnsMetric", metric_vis("m", major),
        {"m": count("5xx")}, ["m"], (10, 7, 10, 7), query=f"{F['status']} >= 500")
    # 499 is nginx's client-closed-connection: a viewer that gave up mid-stream
    # rather than anything x2s3 did wrong, so it is worth its own number.
    add("x2s3-client-aborts", "Client aborts (499)", "lnsMetric", metric_vis("m", major),
        {"m": count("499")}, ["m"], (20, 7, 10, 7), query=f"{F['status']}: 499")

    # Three stacked full-width rows, meant to be read against each other.
    add("x2s3-rate-by-status", "Request rate by status class", "lnsXY",
        xy("bar_stacked", "t", ["m"], split="s"),
        {"t": date_histogram(), "s": status_classes(), "m": count()}, ["t", "s", "m"],
        (0, 14, 48, 12))
    add("x2s3-by-node", "Requests by node", "lnsXY",
        xy("bar_stacked", "t", ["m"], split="h"),
        {"t": date_histogram(), "h": terms(F["host"], 5, "m", "Node"), "m": count()},
        ["t", "h", "m"], (0, 26, 48, 12))
    add("x2s3-unique-clients-over-time", "Unique clients over time", "lnsXY",
        xy("line", "t", ["m"], legend=None),
        {"t": date_histogram(), "m": unique(F["client"], "Unique IPs")}, ["t", "m"],
        (0, 38, 48, 12))

    add("x2s3-by-client-type", "Traffic by client type", "lnsXY",
        xy("bar_stacked", "t", ["m"], split="c"),
        {"t": date_histogram(), "c": client_classes(), "m": count()}, ["t", "c", "m"],
        (0, 50, 48, 12))

    # Latency and throughput.
    add("x2s3-latency-percentiles", "Latency percentiles", "lnsXY",
        xy("line", "t", ["p50", "p95", "p99"]),
        {"t": date_histogram(), "p50": percentile(50, "p50"),
         "p95": percentile(95, "p95"), "p99": percentile(99, "p99")},
        ["t", "p50", "p95", "p99"], (0, 62, 48, 13))
    add("x2s3-throughput", "Throughput", "lnsXY",
        xy("area", "t", ["m"], legend=None),
        {"t": date_histogram(), "m": total_bytes("Bytes")}, ["t", "m"], (0, 75, 48, 11))

    # Per-target and per-client detail.
    add("x2s3-target-table", "Targets: traffic and latency", "lnsDatatable",
        {"layerId": "l1", "layerType": "data",
         "columns": [{"columnId": c} for c in ("b", "m", "p95", "bytes")]},
        {"b": terms(F["target"], 20, "m", "Target"), "m": count(),
         "p95": percentile(95), "bytes": total_bytes()}, ["b", "m", "p95", "bytes"],
        (0, 86, 25, 14))
    add("x2s3-client-table", "Top clients", "lnsDatatable",
        {"layerId": "l1", "layerType": "data",
         "columns": [{"columnId": c} for c in ("b", "m", "bytes", "errors")]},
        {"b": terms(F["client"], 20, "m", "Client IP"), "m": count(),
         "bytes": total_bytes(),
         "errors": count_where("Errors", f"{F['status']} >= 400")},
        ["b", "m", "bytes", "errors"], (25, 86, 23, 14))

    add("x2s3-user-agents", "User agents by unique clients", "lnsDatatable",
        {"layerId": "l1", "layerType": "data",
         "columns": [{"columnId": c} for c in ("b", "ips", "m", "bytes", "errors")]},
        {"b": terms(F["agent"], 25, "ips", "User agent"),
         "ips": unique(F["client"], "Unique IPs"), "m": count(),
         "bytes": total_bytes(),
         "errors": count_where("Errors", f"{F['status']} >= 400")},
        ["b", "ips", "m", "bytes", "errors"], (0, 100, 48, 16))

    # The actual failing requests.
    objects.append(({"id": "x2s3-errors-search", "type": "search", "attributes": {
        "title": "Failed requests (4xx/5xx)",
        "description": "",
        "columns": [F["status"], F["method"], F["path"], F["target"], F["client"], F["trace"]],
        "sort": [["@timestamp", "desc"]],
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
            "query": {"query": f"{F['status']} >= 400", "language": "kuery"},
            "filter": [],
            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"})},
    }, "references": [{"type": "index-pattern", "id": dv,
                       "name": "kibanaSavedObjectMeta.searchSourceJSON.index"}]},
        (0, 116, 48, 18)))

    panels, references = [], []
    for index, (obj, (x, y, w, h)) in enumerate(objects, start=1):
        panels.append({"panelIndex": str(index), "type": obj["type"],
                       "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(index)},
                       "embeddableConfig": {"enhancements": {}},
                       "panelRefName": f"panel_{index}"})
        references.append({"name": f"{index}:panel_{index}", "type": obj["type"], "id": obj["id"]})

    dashboard = {"id": "x2s3-overview", "type": "dashboard", "references": references,
                 "attributes": {
        "title": "x2s3 — Production Overview",
        "description": "Traffic, latency, throughput and errors from the x2s3 access log.",
        "panelsJSON": json.dumps(panels),
        "optionsJSON": json.dumps({"hidePanelTitles": False, "useMargins": True,
                                   "syncColors": False, "syncCursor": True,
                                   "syncTooltips": True}),
        "timeRestore": True, "timeFrom": "now-24h", "timeTo": "now",
        "refreshInterval": {"pause": False, "value": 60000},
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
            "query": {"query": DASHBOARD_QUERY, "language": "kuery"},
            "filter": [exclude_filter("Exclude monitoring bots", MONITORING, dv, 0),
                       exclude_filter("Exclude SDK & tool clients", TOOLS, dv, 1)]})},
        "version": 1}}
    return "\n".join(json.dumps(obj) for obj, _ in objects + [(dashboard, None)])


def push(ndjson):
    """Create the objects directly rather than through _import.

    _import runs the type's migration chain, and a hand-written Lens object with
    no migration version makes Kibana replay its pre-8 migrations and 500.
    _bulk_create stamps the running version instead, so the state is taken as-is.
    """
    objects = [json.loads(line) for line in ndjson.splitlines()]

    # The time range and refresh interval are dashboard settings someone may have
    # changed in the UI; keep whatever is there rather than resetting it.
    existing = call("/api/saved_objects/dashboard/x2s3-overview", missing_ok=True)
    if existing:
        kept = {key: existing["attributes"][key]
                for key in ("timeFrom", "timeTo", "refreshInterval")
                if key in existing["attributes"]}
        for obj in objects:
            if obj["type"] == "dashboard":
                obj["attributes"].update(kept)
        print(f"  keeping time range {kept.get('timeFrom')} -> {kept.get('timeTo')}")
    result = call("/api/saved_objects/_bulk_create?overwrite=true",
                  data=json.dumps(objects).encode(),
                  headers={"Content-Type": "application/json"})
    failed = [o for o in result["saved_objects"] if o.get("error")]
    for obj in result["saved_objects"]:
        print(f"  {obj['type']:10} {obj['id']:26} "
              f"{json.dumps(obj['error']) if obj.get('error') else 'ok'}")
    if failed:
        sys.exit(f"{len(failed)} object(s) failed")
    print(f"\n{BASE}/s/{SPACE}/app/dashboards#/view/x2s3-overview")


if __name__ == "__main__":
    load_dotenv(os.path.join(os.path.dirname(__file__), os.pardir, "var", "kibana.env"))
    command = sys.argv[1] if len(sys.argv) > 1 else "probe"
    if command == "probe":
        probe()
    elif command == "dump":
        print(build(data_view_id(), kibana_major()))
    elif command == "push":
        push(build(data_view_id(), kibana_major()))
    else:
        sys.exit(__doc__)
