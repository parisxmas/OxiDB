# ColdChain — every OxiDB engine, one process

A cold-chain compliance demo: sensors in trucks report temperature, and two
years later you have to **prove** a shipment never left its contracted range.

The point is not that OxiDB *can* do these things. It is that this application
is an ordinary .NET application — EF Core, MQTTnet, StackExchange.Redis, the
AWS SDK — and every one of those libraries is pointed at **the same single
binary**. Nothing here is an OxiDB-specific client except the document and
time-series calls.

Normally you would deploy six systems:

| This demo uses | Instead of | For |
|---|---|---|
| MQTT broker | Mosquitto | sensors publishing readings |
| Time-series engine | InfluxDB | the readings themselves |
| OxiMem (RESP) | Redis | live state + pub/sub |
| SQL engine | PostgreSQL | shipments, customers, penalties |
| Document engine | MongoDB | raw events, verbatim |
| S3 API | MinIO | certificates and photos |

## Why this domain

Because compliance justifies the features a normal demo cannot. "Replay what
this probe reported during a three-hour window fourteen months ago, and prove
the record wasn't altered" is a real question in pharma logistics, and it is
what point-in-time recovery, the audit log, WORM storage and encryption at rest
are *for*. Most demos have no reason to switch them on.

## Which engine holds what, and why

Each engine is here because it is the right tool, not to complete a set. The
test applied to every one: *would you reach for this if OxiDB didn't bundle it?*

- **Time-series** — millions of readings. Gorilla compression and rollups mean
  a month of 10-second samples answers as a chart without a table scan.
- **SQL** — shipments, customers, excursions. Genuinely relational: a breach
  joins to a shipment, which joins to a customer, who has a contracted penalty.
  EF Core is how .NET teams already write this.
- **Document** — the event exactly as the device sent it. The fleet is three
  vendors and no two agree: one probe reports a temperature and nothing else, a
  newer one adds humidity, a door switch and a nested GPS fix, and a third
  vendor calls its fields `sensor_id`/`temp_c` and carries an alarm list. No
  fixed schema fits all three, and the extra fields are not noise — when
  probe-04 breaches, the time-series shows the number climbing and only the
  document shows `door_open: true`, which is *why*. Flattening events into
  columns chosen today throws that away permanently.

  Ingest normalises what the numeric engines need (that mapping is the only
  code that knows the dialects exist) and stores the original untouched. Live
  state in OxiMem is the normalised view — a dashboard should not have to learn
  three vendors' field names — while the document engine keeps what was said.
- **OxiMem** — "what is probe-04 doing right now", plus the flag that keeps one
  failing sensor from writing a breach row every second. State with a TTL: a
  probe silent for five minutes should read as *unknown*, not as its last
  temperature.
- **MQTT** — how sensors talk. Retained messages carry the gateway's status to
  whoever connects next; a Last Will announces it if the gateway dies.

The six probes are deliberately not six copies of each other. Each sits at its
own point inside its contracted band — a probe by the door runs warmer than one
deep in the load, and both are compliant — with its own compressor duty cycle,
so a healthy trace is a slow wave rather than a flat line. The two broken units
fail in different shapes and on coprime periods (173 and 211 ticks), so they
never resynchronise and the pattern takes twenty hours to repeat. A door left
ajar is a sharp rise and a long clawback that only just breaches; a failing
compressor is a slow climb, a plateau at ambient where it is simply not cooling,
then a fast drop when it catches. Telling those two apart from the trace alone
is the entire job of the person reading the chart.
- **S3** — the certificates. A certificate is prose a person signs and an auditor
  reads: its value *is* the text, which is why it belongs in a blob store and not
  a column. The unplanned half is that the engine full-text indexes an object's
  text on PUT without being asked, so the dashboard searches *inside* the
  certificates — "nordfresh breached" ranks the one matching both terms above the
  ones matching either.

Deliberately **not** used: vector search would be a stretch here, and WASM
belongs in a different demo. Forcing them in would make the showcase less
convincing, not more.

## Run it

```bash
# 1. one binary, every engine
OXIDB_SQL=1 OXIDB_TSDB=1 OXIDB_DATA=./data \
OXIDB_ADDR=127.0.0.1:4444 OXIDB_MQTT_PORT=1883 \
OXIDB_OXIMEM_PORT=6379 OXIDB_S3_PORT=9000 OXIDB_AUDIT=1 \
  oxidb-server

# 2. schema + shipments (EF Core creates the tables)
dotnet run --project ColdChain.Api -- seed

# 3. the fan-out: MQTT in, four engines out
dotnet run --project ColdChain.Ingest &

# 4. the fleet (six probes, each its own unit; two of them are broken)
dotnet run --project ColdChain.Simulator

# 5. the API
dotnet run --project ColdChain.Api
```

## Live

<https://coldchain.baltavista.com> — the compose stack below, running.

## Deploying it

```bash
docker compose up -d --build
```

Four containers on an internal network: the released `oxidb-server` (a static
musl build, fetched and run unmodified), the API, the ingest service and the
fleet simulator. **Exactly one port is published**, the API's, bound to
loopback — the host it runs on already has 6379, 5432, 8080 and 27017 taken by
other things, and an internal network means none of that has to be negotiated.
nginx terminates TLS and proxies to it, with buffering off on `/stream` so the
live feed streams instead of being collected into a response that never arrives.

## The dashboard

`dotnet run --project ColdChain.Api`, then open <http://localhost:5077>.

Every panel names the engine that answered it, because that is the whole point:

- **Live probes** — OxiMem. Ingest `PUBLISH`es each reading; the API relays that
  channel to the page as Server-Sent Events. Nothing polls — a tile changes
  because a sensor published, one hop away. A probe that goes quiet fades out
  rather than showing a stale temperature, the same rule as the TTL on its key.
- **Temperature** — the time-series engine, downsampled into 5-second buckets.
  The contracted range is drawn as a band and the readings outside it are marked
  in red, so a breach is visible without reading a number. Click any probe.
- **Raw events** — the document engine. The fields no other engine holds are
  picked out: humidity, a door switch, a GPS fix, another vendor's alarms. Put
  it next to the chart above and the pair tells the whole story — the chart
  shows probe-04's temperature leaving its band, the events show `door_open:
  true` next to it.
- **Shipments** — the SQL engine through EF Core. The penalty column is the
  reason this half is relational: the same breach costs 2,500 for the pharma
  customer and 400 for the grocer, because it joins to *their* contract.

## The one request that needs all of it

```
GET /audit/4
```

```
  shipment : SHP-1004 · Nordfresh Foods
  contract : -20..-15°C
  VERDICT  : BREACHED
    breach → -14.8°C (limit -15)          ← SQL: joined to the customer's contract
  cost     : 400                          ← SQL: penalty × breaches
  evidence :
    raw events kept (document) : 27       ← document: verbatim, as sent
    peak celsius (time-series) : -14.23   ← time-series: max over the journey
    chart points (downsampled) : 30       ← time-series: rolled up for the chart
    certificate (S3)           : 63 bytes ← S3: the signed paperwork
```

One request. One process. Five stores' worth of work.

Other endpoints: `GET /shipments` (SQL), `GET /live` (OxiMem),
`GET /history/{device}` (time-series), `POST /certificate/{id}` (S3).

## Does it grow?

Yes — measured on the deployed stack, ~225 MB/day, ~80 GB/year unbounded. Each
line needed a different answer, and one of them is an engine limitation rather
than a configuration mistake:

| what | was | now bounded by |
|---|---|---|
| raw events (document) | 131 MB/day | a 2-day TTL — but see the WAL, below |
| audit log | 62 MB/day | `OXIDB_AUDIT_MAX_BYTES` + gzip (~100 MB) |
| time-series | 32 MB/day | a 1-minute rollup kept forever, raw dropped after 30 days |
| docker logs | unbounded | `max-size: 10m, max-file: 3` |

The time-series answer is the interesting one, because it is also the *correct*
one: nobody asks for the reading from 09:41:22 two years ago, they ask whether
the load stayed in range. A one-minute rollup of mean/min/max is ~30× smaller
and still proves the case — `max` over a minute cannot hide a breach. So the
rollup is kept forever and the raw stream is dropped after 30 days. That is not
a compromise to save disk; it is the retention rule the regulation implies.

**What this demo changed in the engine:** the document engine used to truncate
its write-ahead log only on a graceful shutdown. It deliberately refused to
checkpoint online — an earlier attempt lost acknowledged writes, and the code
said so plainly. So the live data was bounded by the TTL and the WAL was not:
~130 MB/day, forever. The mitigation was a weekly `docker compose restart`,
because `SIGTERM` runs the final checkpoint.

A TTL does **not** save you: eviction is logical, and in the default (in-RAM)
mode `compact` is a no-op — auto-compaction only applies to disk-first storage.
Measured: 4,000 documents, TTL'd to zero, and the directory did not shrink by a
byte until the process exited.

Restarting a database on a cron schedule to reclaim disk is not an answer, so
0.36.1 implemented online checkpointing properly (`OXIDB_WAL_CHECKPOINT_BYTES`,
default 64 MiB; this stack runs 16 MiB). The reason the old attempt lost writes
is that a writer appends to the WAL and *then* applies to the tree — seal the
WAL in that window and the write is in neither. The fix is a barrier the
checkpoint drains before sealing, so no write is ever mid-flight across it. The
cron job is gone.


## Three things this demo found

Writing it surfaced real gaps, which is what a showcase is good for:

1. **There was no TSDB helper in the .NET packages** — so this demo wrapped
   `ExecRawAsync`, the client's escape hatch, in a shim of its own. That shim
   is now gone: `OxiDb.Client.Tcp` has a typed time-series surface
   (`TsdbWriteAsync`/`TsdbQueryAsync`/rollups/retention), and the demo uses it.
   Writing the shim is what showed it was needed; deleting it is what showed
   the real one was enough.
2. **The AWS .NET SDK rejected OxiDB's ETag** — and it was right to. S3 defines
   an ETag as an MD5 and every client enforces it; OxiDB's was the first 16
   bytes of a SHA-256, which is the same *shape* as an MD5 (32 hex characters).
   That is what made it harmful rather than merely non-standard: a client cannot
   tell it apart, so it concludes the upload corrupted. Fixed in 0.36.2 — the
   ETag is an MD5, `DisableMD5Stream` is gone, and the fix exposed that
   multipart was never S3-shaped either (its part tags were CRC32s).
3. **The unbounded WAL**, above — the demo is the reason the engine can now
   checkpoint while open. Running something for weeks asks questions a
   benchmark never does.

## It survives its own dependencies restarting

The engine restarting underneath the pipeline — a deploy, a checkpoint, a crash
— broke ingest silently: MQTT dropped, the OxiDB TCP socket died with it, and
MQTTnet swallows exceptions thrown from a message handler. Every container went
on reporting healthy while nothing was written. That is the worst failure mode
there is, because it looks fine.

So both connections now heal themselves (MQTT reconnects *and re-subscribes* —
subscriptions do not survive the broker; the OxiDB client is rebuilt on demand),
and the handler can no longer fail quietly. Verified the way it should be: kill
the engine under the running stack and watch the dashboard come back **without
touching anything**. It takes about ten seconds.

> OxiDB is under active development and not yet recommended for production use.
