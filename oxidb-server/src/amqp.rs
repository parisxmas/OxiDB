//! AMQP 0-9-1 connection handler. ADR-0016 Phases 1–2.
//!
//! Mirrors the MQTT handler's shape: one thread per connection, a 50ms-timeout
//! read loop that alternates between draining broker deliveries and reading
//! client frames. The state that must outlive a connection lives in
//! `AmqpBroker` (`amqp_queue.rs`); what lives here is per-connection: the
//! channel map, unacked deliveries (delivery tags are PER CHANNEL in AMQP),
//! and the in-progress publish being reassembled from header + body frames.
//!
//! Anything outside the implemented subset is answered with a channel error
//! naming the ADR — the honest refusal the ADR requires — never accepted
//! silently.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::amqp_queue::{AmqpBroker, ChannelError, NOT_IMPLEMENTED, QMsg};
use crate::amqp_wire::*;

/// Our side of the Tune negotiation. Frame-max leaves room for large bodies
/// without letting a client demand unbounded buffers.
const FRAME_MAX: u32 = 131_072;
const CHANNEL_MAX: u16 = 2047;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_CTAG: AtomicU64 = AtomicU64::new(1);

/// A publish mid-reassembly: method seen, waiting on header and body frames.
struct Pending {
    exchange: String,
    routing_key: String,
    /// The publish's mandatory flag: an unroutable message comes back as
    /// Basic.Return instead of being dropped.
    mandatory: bool,
    header: Option<ContentHeader>,
    body: Vec<u8>,
}

#[derive(Default)]
struct Ch {
    confirms: bool,
    /// Confirm-mode publish counter — the delivery-tag space of Basic.Ack
    /// TO the publisher. Starts at 1 after Confirm.Select, per spec.
    publish_seq: u64,
    next_dtag: u64,
    /// delivery-tag → (queue, message, counted). Requeued wholesale if the
    /// channel or connection dies — write-before-ack's other half. `counted`
    /// marks deliveries holding a Basic.Qos prefetch slot (consume path only;
    /// the spec exempts Basic.Get), which settling must release.
    unacked: BTreeMap<u64, (String, QMsg, bool)>,
    pending: Option<Pending>,
    consumer_tags: Vec<String>,
    /// We sent Channel.Close (a channel error) and await CloseOk; frames on
    /// the channel are ignored until then, per spec.
    closing: bool,
}

/// Read one frame with poll semantics: 50ms for the first byte (so the loop
/// can interleave deliveries), then a generous timeout for the REST of the
/// frame — once a frame has started, a mid-frame timeout would desynchronise
/// the stream permanently, so the tail must be allowed to arrive.
fn read_frame_polled(
    stream: &TcpStream,
    r: &mut BufReader<&TcpStream>,
    max: usize,
) -> io::Result<Option<Frame>> {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
    let mut first = [0u8; 1];
    match r.read_exact(&mut first) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            return Ok(None);
        }
        Err(e) => return Err(e),
    }
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut rest = [0u8; 6];
    r.read_exact(&mut rest)?;
    let channel = u16::from_be_bytes([rest[0], rest[1]]);
    let size = u32::from_be_bytes([rest[2], rest[3], rest[4], rest[5]]) as usize;
    if size > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds negotiated max",
        ));
    }
    let mut payload = vec![0u8; size];
    r.read_exact(&mut payload)?;
    let mut end = [0u8; 1];
    r.read_exact(&mut end)?;
    if end[0] != FRAME_END {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frame-end",
        ));
    }
    Ok(Some(Frame {
        kind: first[0],
        channel,
        payload,
    }))
}

/// Send a channel exception and put the channel into closing state: requeue
/// its unacked deliveries and cancel its consumers first, per spec semantics.
fn channel_error(
    w: &mut BufWriter<&TcpStream>,
    broker: &AmqpBroker,
    conn_id: u64,
    channels: &mut HashMap<u16, Ch>,
    ch_id: u16,
    err: ChannelError,
    class: u16,
    method: u16,
) -> io::Result<()> {
    if let Some(ch) = channels.get_mut(&ch_id) {
        for (_dtag, (queue, msg, _counted)) in std::mem::take(&mut ch.unacked) {
            broker.requeue(&queue, msg);
        }
        for ctag in ch.consumer_tags.drain(..) {
            broker.cancel_consumer(conn_id, &ctag);
        }
        broker.channel_closed(conn_id, ch_id);
        ch.closing = true;
        ch.pending = None;
    }
    let p = ArgsW::method(CLASS_CHANNEL, CHANNEL_CLOSE)
        .u16(err.0)
        .shortstr(&err.1)
        .u16(class)
        .u16(method)
        .finish();
    write_frame(w, FRAME_METHOD, ch_id, &p)?;
    w.flush()
}

/// Write one message to a consumer: Deliver (or GetOk) + header + body frames,
/// splitting the body at the negotiated frame size.
fn write_content(
    w: &mut BufWriter<&TcpStream>,
    ch_id: u16,
    method_payload: &[u8],
    props_raw: &[u8],
    body: &[u8],
    frame_max: usize,
) -> io::Result<()> {
    write_frame(w, FRAME_METHOD, ch_id, method_payload)?;
    let header = build_content_header(body.len() as u64, props_raw);
    write_frame(w, FRAME_HEADER, ch_id, &header)?;
    // Body frames: frame overhead is 8 bytes (7 header + 1 end).
    let chunk = frame_max.saturating_sub(8).max(1);
    for part in body.chunks(chunk) {
        write_frame(w, FRAME_BODY, ch_id, part)?;
    }
    Ok(())
}

pub fn handle_client(stream: TcpStream, log: bool) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::SeqCst);
    let broker = AmqpBroker::global();
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    // ── Handshake ───────────────────────────────────────────────────────
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let mut proto = [0u8; 8];
    if reader.read_exact(&mut proto).is_err() {
        return;
    }
    if proto != PROTOCOL_HEADER {
        // Spec: answer a version we DO speak and close, so the client can
        // report a version mismatch instead of a mystery hangup.
        let _ = writer.write_all(&PROTOCOL_HEADER);
        let _ = writer.flush();
        return;
    }

    let start = ArgsW::method(CLASS_CONNECTION, CONNECTION_START)
        .u8(0)
        .u8(9)
        .table_raw(&server_properties())
        .longstr(b"PLAIN")
        .longstr(b"en_US")
        .finish();
    if write_frame(&mut writer, FRAME_METHOD, 0, &start).is_err() || writer.flush().is_err() {
        return;
    }

    // StartOk: client-properties (skipped), mechanism, response, locale.
    let (user, pass) = match read_frame(&mut reader, FRAME_MAX as usize) {
        Ok(f) if f.kind == FRAME_METHOD => {
            let mut a = Args::new(&f.payload);
            let ok =
                a.u16().ok() == Some(CLASS_CONNECTION) && a.u16().ok() == Some(CONNECTION_START_OK);
            if !ok {
                return;
            }
            if a.skip_table().is_err() {
                return;
            }
            let _mechanism = a.shortstr().unwrap_or_default();
            let response = a.longstr().unwrap_or_default();
            // PLAIN: \0user\0password
            let mut parts = response.split(|b| *b == 0);
            let _ = parts.next();
            let user = String::from_utf8_lossy(parts.next().unwrap_or(&[])).to_string();
            let pass = String::from_utf8_lossy(parts.next().unwrap_or(&[])).to_string();
            (user, pass)
        }
        _ => return,
    };
    if let (Ok(want_u), Ok(want_p)) = (
        std::env::var("OXIDB_AMQP_USER"),
        std::env::var("OXIDB_AMQP_PASSWORD"),
    ) {
        if user != want_u || pass != want_p {
            let p = ArgsW::method(CLASS_CONNECTION, CONNECTION_CLOSE)
                .u16(403)
                .shortstr("ACCESS_REFUSED - PLAIN login refused")
                .u16(0)
                .u16(0)
                .finish();
            let _ = write_frame(&mut writer, FRAME_METHOD, 0, &p);
            let _ = writer.flush();
            return;
        }
    }

    let tune = ArgsW::method(CLASS_CONNECTION, CONNECTION_TUNE)
        .u16(CHANNEL_MAX)
        .u32(FRAME_MAX)
        .u16(60) // heartbeat proposal; the client's TuneOk decides
        .finish();
    if write_frame(&mut writer, FRAME_METHOD, 0, &tune).is_err() || writer.flush().is_err() {
        return;
    }

    let (frame_max, heartbeat) = match read_frame(&mut reader, FRAME_MAX as usize) {
        Ok(f) if f.kind == FRAME_METHOD => {
            let mut a = Args::new(&f.payload);
            let ok =
                a.u16().ok() == Some(CLASS_CONNECTION) && a.u16().ok() == Some(CONNECTION_TUNE_OK);
            if !ok {
                return;
            }
            let _channel_max = a.u16().unwrap_or(CHANNEL_MAX);
            let fm = a.u32().unwrap_or(FRAME_MAX);
            let hb = a.u16().unwrap_or(0);
            // 0 from the client means "no limit" — ours then applies.
            let fm = if fm == 0 {
                FRAME_MAX
            } else {
                fm.min(FRAME_MAX)
            };
            (fm as usize, hb)
        }
        _ => return,
    };

    // Connection.Open (vhost accepted and ignored — one namespace).
    match read_frame(&mut reader, frame_max) {
        Ok(f) if f.kind == FRAME_METHOD => {
            let mut a = Args::new(&f.payload);
            let ok =
                a.u16().ok() == Some(CLASS_CONNECTION) && a.u16().ok() == Some(CONNECTION_OPEN);
            if !ok {
                return;
            }
            let openok = ArgsW::method(CLASS_CONNECTION, CONNECTION_OPEN_OK)
                .shortstr("")
                .finish();
            if write_frame(&mut writer, FRAME_METHOD, 0, &openok).is_err()
                || writer.flush().is_err()
            {
                return;
            }
        }
        _ => return,
    }

    if log {
        eprintln!(
            "[amqp] open conn={conn_id} peer={peer} frame_max={frame_max} heartbeat={heartbeat}"
        );
    }

    // ── Main loop ───────────────────────────────────────────────────────
    let mut channels: HashMap<u16, Ch> = HashMap::new();
    let mut last_write = Instant::now();

    'conn: loop {
        // Deliveries owed to this connection's consumers.
        let outs = broker.poll(conn_id);
        let mut wrote = false;
        for out in outs {
            let Some(ch) = channels.get_mut(&out.channel) else {
                // The channel died between registration and delivery — the
                // message must not vanish with it (and the prefetch slot the
                // poll charged must not leak).
                if !out.no_ack {
                    broker.settle(conn_id, out.channel);
                }
                broker.requeue(&out.queue, out.msg);
                continue;
            };
            if ch.closing {
                if !out.no_ack {
                    broker.settle(conn_id, out.channel);
                }
                broker.requeue(&out.queue, out.msg);
                continue;
            }
            ch.next_dtag += 1;
            let dtag = ch.next_dtag;
            let deliver = ArgsW::method(CLASS_BASIC, BASIC_DELIVER)
                .shortstr(&out.ctag)
                .u64(dtag)
                .u8(out.msg.redelivered as u8)
                .shortstr(&out.msg.exchange)
                .shortstr(&out.msg.routing_key)
                .finish();
            if write_content(
                &mut writer,
                out.channel,
                &deliver,
                &out.msg.props_raw,
                &out.msg.body,
                frame_max,
            )
            .is_err()
            {
                if !out.no_ack {
                    broker.settle(conn_id, out.channel);
                }
                broker.requeue(&out.queue, out.msg);
                break 'conn;
            }
            if !out.no_ack {
                ch.unacked.insert(dtag, (out.queue, out.msg, true));
            }
            wrote = true;
        }
        if wrote && writer.flush().is_err() {
            break 'conn;
        }
        if wrote {
            last_write = Instant::now();
        }

        // Heartbeat: the negotiated interval is a promise to emit SOMETHING
        // within it; half-interval keeps us safely inside.
        if heartbeat > 0
            && last_write.elapsed() > Duration::from_secs((heartbeat / 2).max(1) as u64)
        {
            if write_frame(&mut writer, FRAME_HEARTBEAT, 0, &[]).is_err() || writer.flush().is_err()
            {
                break 'conn;
            }
            last_write = Instant::now();
        }

        let frame = match read_frame_polled(&stream, &mut reader, frame_max) {
            Ok(Some(f)) => f,
            Ok(None) => continue,
            Err(_) => break 'conn,
        };

        match frame.kind {
            FRAME_HEARTBEAT => {}

            FRAME_HEADER => {
                let ch_id = frame.channel;
                let Some(ch) = channels.get_mut(&ch_id) else {
                    continue;
                };
                if ch.closing {
                    continue;
                }
                let Some(pending) = ch.pending.as_mut() else {
                    continue;
                };
                match parse_content_header(&frame.payload) {
                    Ok(h) => {
                        let complete = h.body_size == 0;
                        pending.header = Some(h);
                        if complete
                            && finish_publish(
                                &mut writer,
                                broker,
                                conn_id,
                                &mut channels,
                                ch_id,
                                frame_max,
                            )
                            .is_err()
                        {
                            break 'conn;
                        }
                    }
                    Err(_) => break 'conn,
                }
            }

            FRAME_BODY => {
                let ch_id = frame.channel;
                let Some(ch) = channels.get_mut(&ch_id) else {
                    continue;
                };
                let Some(pending) = ch.pending.as_mut() else {
                    continue;
                };
                pending.body.extend_from_slice(&frame.payload);
                let done = pending
                    .header
                    .as_ref()
                    .is_some_and(|h| pending.body.len() as u64 >= h.body_size);
                if done
                    && finish_publish(
                        &mut writer,
                        broker,
                        conn_id,
                        &mut channels,
                        ch_id,
                        frame_max,
                    )
                    .is_err()
                {
                    break 'conn;
                }
            }

            FRAME_METHOD => {
                let mut a = Args::new(&frame.payload);
                let (Ok(class), Ok(method)) = (a.u16(), a.u16()) else {
                    break 'conn;
                };
                let ch_id = frame.channel;

                // Connection-class methods ride channel 0.
                if class == CLASS_CONNECTION {
                    match method {
                        CONNECTION_CLOSE => {
                            let ok = ArgsW::method(CLASS_CONNECTION, CONNECTION_CLOSE_OK).finish();
                            let _ = write_frame(&mut writer, FRAME_METHOD, 0, &ok);
                            let _ = writer.flush();
                            break 'conn;
                        }
                        CONNECTION_CLOSE_OK => break 'conn,
                        _ => break 'conn,
                    }
                }

                // A channel in closing state ignores everything except CloseOk.
                if channels.get(&ch_id).is_some_and(|c| c.closing) {
                    if class == CLASS_CHANNEL && method == CHANNEL_CLOSE_OK {
                        channels.remove(&ch_id);
                    }
                    continue;
                }

                let r: io::Result<()> = handle_method(
                    &mut writer,
                    broker,
                    conn_id,
                    &mut channels,
                    ch_id,
                    class,
                    method,
                    &mut a,
                    frame_max,
                );
                if r.is_err() {
                    break 'conn;
                }
            }

            _ => break 'conn,
        }
    }

    // ── Teardown: nothing a consumer failed to ack may be lost ─────────
    for (_id, ch) in channels.iter_mut() {
        for (_dtag, (queue, msg, _counted)) in std::mem::take(&mut ch.unacked) {
            broker.requeue(&queue, msg);
        }
    }
    broker.connection_closed(conn_id);
    if log {
        eprintln!("[amqp] closed conn={conn_id} peer={peer}");
    }
}

/// A completed publish: route it, and in confirm mode acknowledge it — AFTER
/// the broker call, whose durable insert is fsync'd. Write-before-confirm.
/// An unroutable mandatory publish comes back as Basic.Return (312 NO_ROUTE)
/// BEFORE the confirm — pika collects the return, sees the ack, and raises
/// UnroutableError, matching RabbitMQ's ordering.
fn finish_publish(
    writer: &mut BufWriter<&TcpStream>,
    broker: &AmqpBroker,
    conn_id: u64,
    channels: &mut HashMap<u16, Ch>,
    ch_id: u16,
    frame_max: usize,
) -> io::Result<()> {
    let Some(ch) = channels.get_mut(&ch_id) else {
        return Ok(());
    };
    let Some(p) = ch.pending.take() else {
        return Ok(());
    };
    let dm = p.header.as_ref().and_then(|h| h.delivery_mode);
    let props = p.header.map(|h| h.props_raw).unwrap_or_default();
    // A mandatory publish may need its content handed back; the clone is paid
    // only when the flag is set.
    let ret = p.mandatory.then(|| (props.clone(), p.body.clone()));
    match broker.publish(&p.exchange, &p.routing_key, props, p.body, dm) {
        Ok(routed) => {
            if routed == 0 {
                if let Some((rprops, rbody)) = ret {
                    let m = ArgsW::method(CLASS_BASIC, BASIC_RETURN)
                        .u16(312)
                        .shortstr("NO_ROUTE")
                        .shortstr(&p.exchange)
                        .shortstr(&p.routing_key)
                        .finish();
                    write_content(writer, ch_id, &m, &rprops, &rbody, frame_max)?;
                    // Flush HERE, not only via the confirm below: on a
                    // non-confirm channel there is no ack to ride out on, and
                    // an unflushed Return sits invisible until the next
                    // heartbeat. Found by RabbitMQ.Client (.NET), which
                    // listens for returns without confirm mode — pika's
                    // confirm-mode flush masked it.
                    writer.flush()?;
                }
            }
            if ch.confirms {
                ch.publish_seq += 1;
                let ack = ArgsW::method(CLASS_BASIC, BASIC_ACK)
                    .u64(ch.publish_seq)
                    .u8(0) // multiple = false
                    .finish();
                write_frame(writer, FRAME_METHOD, ch_id, &ack)?;
                writer.flush()?;
            }
            Ok(())
        }
        Err(e) => channel_error(
            writer,
            broker,
            conn_id,
            channels,
            ch_id,
            e,
            CLASS_BASIC,
            BASIC_PUBLISH,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_method(
    writer: &mut BufWriter<&TcpStream>,
    broker: &AmqpBroker,
    conn_id: u64,
    channels: &mut HashMap<u16, Ch>,
    ch_id: u16,
    class: u16,
    method: u16,
    a: &mut Args<'_>,
    frame_max: usize,
) -> io::Result<()> {
    match (class, method) {
        (CLASS_CHANNEL, CHANNEL_OPEN) => {
            channels.insert(ch_id, Ch::default());
            let p = ArgsW::method(CLASS_CHANNEL, CHANNEL_OPEN_OK)
                .longstr(b"")
                .finish();
            write_frame(writer, FRAME_METHOD, ch_id, &p)?;
            writer.flush()
        }
        (CLASS_CHANNEL, CHANNEL_CLOSE) => {
            if let Some(mut ch) = channels.remove(&ch_id) {
                for (_d, (queue, msg, _counted)) in std::mem::take(&mut ch.unacked) {
                    broker.requeue(&queue, msg);
                }
                for ctag in ch.consumer_tags.drain(..) {
                    broker.cancel_consumer(conn_id, &ctag);
                }
                broker.channel_closed(conn_id, ch_id);
            }
            let p = ArgsW::method(CLASS_CHANNEL, CHANNEL_CLOSE_OK).finish();
            write_frame(writer, FRAME_METHOD, ch_id, &p)?;
            writer.flush()
        }

        (CLASS_EXCHANGE, EXCHANGE_DECLARE) => {
            let _reserved = a.u16()?;
            let name = a.shortstr()?;
            let kind = a.shortstr()?;
            let bits = a.bits()?;
            let durable = bits & 0x02 != 0;
            let no_wait = bits & 0x10 != 0;
            match broker.declare_exchange(&name, &kind, durable) {
                Ok(()) => {
                    if !no_wait {
                        let p = ArgsW::method(CLASS_EXCHANGE, EXCHANGE_DECLARE_OK).finish();
                        write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                        writer.flush()?;
                    }
                    Ok(())
                }
                Err(e) => channel_error(writer, broker, conn_id, channels, ch_id, e, class, method),
            }
        }

        (CLASS_QUEUE, QUEUE_DECLARE) => {
            let _reserved = a.u16()?;
            let name = a.shortstr()?;
            let bits = a.bits()?;
            let (passive, durable) = (bits & 0x01 != 0, bits & 0x02 != 0);
            let (exclusive, auto_delete) = (bits & 0x04 != 0, bits & 0x08 != 0);
            let no_wait = bits & 0x10 != 0;
            match broker.declare_queue(&name, durable, exclusive, auto_delete, passive, conn_id) {
                Ok((qname, msgs, consumers)) => {
                    if !no_wait {
                        let p = ArgsW::method(CLASS_QUEUE, QUEUE_DECLARE_OK)
                            .shortstr(&qname)
                            .u32(msgs)
                            .u32(consumers)
                            .finish();
                        write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                        writer.flush()?;
                    }
                    Ok(())
                }
                Err(e) => channel_error(writer, broker, conn_id, channels, ch_id, e, class, method),
            }
        }

        (CLASS_QUEUE, QUEUE_BIND) => {
            let _reserved = a.u16()?;
            let queue = a.shortstr()?;
            let exchange = a.shortstr()?;
            let key = a.shortstr()?;
            let no_wait = a.bits()? & 0x01 != 0;
            match broker.bind(&queue, &exchange, &key) {
                Ok(()) => {
                    if !no_wait {
                        let p = ArgsW::method(CLASS_QUEUE, QUEUE_BIND_OK).finish();
                        write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                        writer.flush()?;
                    }
                    Ok(())
                }
                Err(e) => channel_error(writer, broker, conn_id, channels, ch_id, e, class, method),
            }
        }

        (CLASS_BASIC, BASIC_PUBLISH) => {
            let _reserved = a.u16()?;
            let exchange = a.shortstr()?;
            let routing_key = a.shortstr()?;
            let bits = a.bits()?;
            let mandatory = bits & 0x01 != 0;
            if bits & 0x02 != 0 {
                // `immediate` was dropped by RabbitMQ itself (3.0). Refusing
                // beats accepting a delivery guarantee we would not honour.
                return channel_error(
                    writer,
                    broker,
                    conn_id,
                    channels,
                    ch_id,
                    (
                        NOT_IMPLEMENTED,
                        "the `immediate` flag is not supported (RabbitMQ dropped it too)".into(),
                    ),
                    class,
                    method,
                );
            }
            if let Some(ch) = channels.get_mut(&ch_id) {
                ch.pending = Some(Pending {
                    exchange,
                    routing_key,
                    mandatory,
                    header: None,
                    body: Vec::new(),
                });
            }
            Ok(())
        }

        (CLASS_BASIC, BASIC_QOS) => {
            let _prefetch_size = a.u32()?; // byte-based windowing: nobody implements it, incl. RabbitMQ
            let count = a.u16()?;
            let _global = a.bits()?; // applied per channel either way — see set_qos
            broker.set_qos(conn_id, ch_id, count);
            let p = ArgsW::method(CLASS_BASIC, BASIC_QOS_OK).finish();
            write_frame(writer, FRAME_METHOD, ch_id, &p)?;
            writer.flush()
        }

        (CLASS_BASIC, BASIC_CONSUME) => {
            let _reserved = a.u16()?;
            let queue = a.shortstr()?;
            let mut ctag = a.shortstr()?;
            let bits = a.bits()?;
            let no_ack = bits & 0x02 != 0;
            let no_wait = bits & 0x08 != 0;
            if ctag.is_empty() {
                ctag = format!("ctag-{}", NEXT_CTAG.fetch_add(1, Ordering::SeqCst));
            }
            match broker.register_consumer(&queue, conn_id, ch_id, ctag.clone(), no_ack) {
                Ok(()) => {
                    if let Some(ch) = channels.get_mut(&ch_id) {
                        ch.consumer_tags.push(ctag.clone());
                    }
                    if !no_wait {
                        let p = ArgsW::method(CLASS_BASIC, BASIC_CONSUME_OK)
                            .shortstr(&ctag)
                            .finish();
                        write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                        writer.flush()?;
                    }
                    Ok(())
                }
                Err(e) => channel_error(writer, broker, conn_id, channels, ch_id, e, class, method),
            }
        }

        (CLASS_BASIC, BASIC_CANCEL) => {
            let ctag = a.shortstr()?;
            let no_wait = a.bits()? & 0x01 != 0;
            broker.cancel_consumer(conn_id, &ctag);
            if let Some(ch) = channels.get_mut(&ch_id) {
                ch.consumer_tags.retain(|t| t != &ctag);
            }
            if !no_wait {
                let p = ArgsW::method(CLASS_BASIC, BASIC_CANCEL_OK)
                    .shortstr(&ctag)
                    .finish();
                write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                writer.flush()?;
            }
            Ok(())
        }

        (CLASS_BASIC, BASIC_GET) => {
            let _reserved = a.u16()?;
            let queue = a.shortstr()?;
            let no_ack = a.bits()? & 0x01 != 0;
            match broker.get_one(&queue, no_ack) {
                Ok(Some((msg, remaining))) => {
                    let ch = channels.entry(ch_id).or_default();
                    ch.next_dtag += 1;
                    let dtag = ch.next_dtag;
                    let getok = ArgsW::method(CLASS_BASIC, BASIC_GET_OK)
                        .u64(dtag)
                        .u8(msg.redelivered as u8)
                        .shortstr(&msg.exchange)
                        .shortstr(&msg.routing_key)
                        .u32(remaining)
                        .finish();
                    write_content(writer, ch_id, &getok, &msg.props_raw, &msg.body, frame_max)?;
                    writer.flush()?;
                    if !no_ack {
                        // counted=false: Basic.Get is exempt from prefetch.
                        ch.unacked.insert(dtag, (queue, msg, false));
                    }
                    Ok(())
                }
                Ok(None) => {
                    let p = ArgsW::method(CLASS_BASIC, BASIC_GET_EMPTY)
                        .shortstr("")
                        .finish();
                    write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                    writer.flush()
                }
                Err(e) => channel_error(writer, broker, conn_id, channels, ch_id, e, class, method),
            }
        }

        (CLASS_BASIC, BASIC_ACK) => {
            let dtag = a.u64()?;
            let multiple = a.bits()? & 0x01 != 0;
            if let Some(ch) = channels.get_mut(&ch_id) {
                let tags: Vec<u64> = if multiple {
                    ch.unacked.range(..=dtag).map(|(t, _)| *t).collect()
                } else {
                    ch.unacked
                        .contains_key(&dtag)
                        .then_some(dtag)
                        .into_iter()
                        .collect()
                };
                for t in tags {
                    if let Some((_q, msg, counted)) = ch.unacked.remove(&t) {
                        broker.ack(msg.seq);
                        if counted {
                            broker.settle(conn_id, ch_id);
                        }
                    }
                }
            }
            Ok(())
        }

        (CLASS_BASIC, BASIC_NACK) | (CLASS_BASIC, BASIC_REJECT) => {
            // Negative acknowledgement: requeue or discard. A discard still
            // deletes the durable record — the consumer has DECIDED about the
            // message, which is what the record was waiting for. (Dead-letter
            // routing is explicitly out of ADR-0016's scope; without it,
            // discard means discard.)
            let dtag = a.u64()?;
            let bits = a.bits()?;
            let (multiple, requeue) = if method == BASIC_NACK {
                (bits & 0x01 != 0, bits & 0x02 != 0)
            } else {
                (false, bits & 0x01 != 0) // Reject: single message only
            };
            if let Some(ch) = channels.get_mut(&ch_id) {
                let tags: Vec<u64> = if multiple {
                    ch.unacked.range(..=dtag).map(|(t, _)| *t).collect()
                } else {
                    ch.unacked
                        .contains_key(&dtag)
                        .then_some(dtag)
                        .into_iter()
                        .collect()
                };
                for t in tags {
                    if let Some((queue, msg, counted)) = ch.unacked.remove(&t) {
                        if requeue {
                            broker.requeue(&queue, msg);
                        } else {
                            broker.ack(msg.seq);
                        }
                        if counted {
                            broker.settle(conn_id, ch_id);
                        }
                    }
                }
            }
            Ok(())
        }

        (CLASS_CONFIRM, CONFIRM_SELECT) => {
            let no_wait = a.bits()? & 0x01 != 0;
            if let Some(ch) = channels.get_mut(&ch_id) {
                ch.confirms = true;
                ch.publish_seq = 0;
            }
            if !no_wait {
                let p = ArgsW::method(CLASS_CONFIRM, CONFIRM_SELECT_OK).finish();
                write_frame(writer, FRAME_METHOD, ch_id, &p)?;
                writer.flush()?;
            }
            Ok(())
        }

        _ => channel_error(
            writer,
            broker,
            conn_id,
            channels,
            ch_id,
            (
                NOT_IMPLEMENTED,
                format!("method {class}.{method} is outside the ADR-0016 subset"),
            ),
            class,
            method,
        ),
    }
}
