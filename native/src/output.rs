//! Parquet output layer: writes trace.parquet / message_trace.parquet so that
//! `pd.read_parquet` returns exactly the DataFrame the Python baseline
//! (`df.to_parquet(path, compression="snappy", index=False)`, pandas 1.5.3 +
//! pyarrow 15) produces: same column order, dtypes (string / Int64), RangeIndex.
//!
//! Metadata is copied verbatim from a pyarrow-15 reference file: the `pandas`
//! key restores `string`/`Int64`; `ARROW:schema` (an IPC schema embedding the
//! same pandas JSON) is included so the arrow schema (incl. metadata) is
//! byte-identical to the reference. Both are constant per file kind
//! (index=False => no row-count dependent metadata).
//! Speed: the parquet crate's generic column writers cost ~110 ms per 1M rows, so the
//! page bytes are encoded here (v1 data pages, uncompressed: snappy saved 2-10% of bytes on
//! these encodings for real CPU time; DELTA_BINARY_PACKED ints,
//! dictionary strings, no statistics) and spliced into a `SerializedFileWriter`
//! with `append_column`, which still writes the footer/schema/metadata.
//! Columns are read through per-row closures (no materialized column vectors), every
//! (row group, column) chunk is encoded as an independent job on `threads` workers, and the
//! file is assembled in memory so its SHA-256 comes without reading it back.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use parquet::basic::{Compression, Encoding};
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{ColumnChunkMetaData, KeyValue};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use sha2::{Digest, Sha256};

pub struct TraceCols {
    pub t_ns: Vec<i64>,
    pub agent_id: Vec<i32>,
    pub msg_type: Vec<&'static str>,
    pub side: Vec<Option<&'static str>>,
    pub price: Vec<i64>,
    pub size: Vec<i64>,
    pub order_id: Vec<i64>,
}

pub struct MsgCols {
    pub seq: Vec<i64>,
    pub t_recv_ns: Vec<i64>,
    pub t_send_ns: Vec<Option<i64>>,
    pub latency_ns: Vec<i64>,
    pub src_id: Vec<i32>,
    pub dst_id: Vec<i32>,
    pub message_id: Vec<i64>,
    pub msg_type: Vec<&'static str>,
    pub order_id: Vec<Option<i64>>,
    pub causal_parent: Vec<Option<i64>>,
}

/// pyarrow's default row-group size (1 Mi rows); the reference files split the same way.
const ROW_GROUP: usize = 1 << 20;
/// Values per data page.
const PAGE_ROWS: usize = 1 << 16;

const TRACE_SCHEMA: &str = "message schema {
  optional int64 t_ns;
  optional int32 agent_id;
  optional binary msg_type (STRING);
  optional binary side (STRING);
  optional int64 price;
  optional int64 size;
  optional int64 order_id;
}";

const MSG_SCHEMA: &str = "message schema {
  optional int64 seq;
  optional int64 t_recv_ns;
  optional int64 t_send_ns;
  optional int64 latency_ns;
  optional int32 src_id;
  optional int32 dst_id;
  optional int64 message_id;
  optional binary msg_type (STRING);
  optional int64 order_id;
  optional int64 causal_parent;
}";

const TRACE_PANDAS: &str = r#"{"index_columns": [], "column_indexes": [], "columns": [{"name": "t_ns", "field_name": "t_ns", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "agent_id", "field_name": "agent_id", "pandas_type": "int32", "numpy_type": "int32", "metadata": null}, {"name": "msg_type", "field_name": "msg_type", "pandas_type": "unicode", "numpy_type": "string", "metadata": null}, {"name": "side", "field_name": "side", "pandas_type": "unicode", "numpy_type": "string", "metadata": null}, {"name": "price", "field_name": "price", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "size", "field_name": "size", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "order_id", "field_name": "order_id", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}], "creator": {"library": "pyarrow", "version": "15.0.2"}, "pandas_version": "1.5.3"}"#;
const TRACE_ARROW: &str = "/////3gFAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAAMADAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAAAIAAAAEAAAAAYAAABwYW5kYXMAAIoDAAB7ImluZGV4X2NvbHVtbnMiOiBbXSwgImNvbHVtbl9pbmRleGVzIjogW10sICJjb2x1bW5zIjogW3sibmFtZSI6ICJ0X25zIiwgImZpZWxkX25hbWUiOiAidF9ucyIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogImludDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJhZ2VudF9pZCIsICJmaWVsZF9uYW1lIjogImFnZW50X2lkIiwgInBhbmRhc190eXBlIjogImludDMyIiwgIm51bXB5X3R5cGUiOiAiaW50MzIiLCAibWV0YWRhdGEiOiBudWxsfSwgeyJuYW1lIjogIm1zZ190eXBlIiwgImZpZWxkX25hbWUiOiAibXNnX3R5cGUiLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogInN0cmluZyIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAic2lkZSIsICJmaWVsZF9uYW1lIjogInNpZGUiLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogInN0cmluZyIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAicHJpY2UiLCAiZmllbGRfbmFtZSI6ICJwcmljZSIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogImludDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJzaXplIiwgImZpZWxkX25hbWUiOiAic2l6ZSIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogImludDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJvcmRlcl9pZCIsICJmaWVsZF9uYW1lIjogIm9yZGVyX2lkIiwgInBhbmRhc190eXBlIjogImludDY0IiwgIm51bXB5X3R5cGUiOiAiaW50NjQiLCAibWV0YWRhdGEiOiBudWxsfV0sICJjcmVhdG9yIjogeyJsaWJyYXJ5IjogInB5YXJyb3ciLCAidmVyc2lvbiI6ICIxNS4wLjIifSwgInBhbmRhc192ZXJzaW9uIjogIjEuNS4zIn0AAAcAAABMAQAABAEAANAAAACkAAAAcAAAADwAAAAEAAAA4P7//wAAAQIQAAAAHAAAAAQAAAAAAAAACAAAAG9yZGVyX2lkAAAAANT+//8AAAABQAAAABT///8AAAECEAAAABgAAAAEAAAAAAAAAAQAAABzaXplAAAAAAT///8AAAABQAAAAET///8AAAECEAAAABgAAAAEAAAAAAAAAAUAAABwcmljZQAAADT///8AAAABQAAAAHT///8AAAEFEAAAABgAAAAEAAAAAAAAAAQAAABzaWRlAAAAANT///+c////AAABBRAAAAAgAAAABAAAAAAAAAAIAAAAbXNnX3R5cGUAAAAABAAEAAQAAADM////AAABAhAAAAAcAAAABAAAAAAAAAAIAAAAYWdlbnRfaWQAAAAAwP///wAAAAEgAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAECEAAAACAAAAAEAAAAAAAAAAQAAAB0X25zAAAAAAgADAAIAAcACAAAAAAAAAFAAAAAAAAAAA==";

const MSG_PANDAS: &str = r#"{"index_columns": [], "column_indexes": [], "columns": [{"name": "seq", "field_name": "seq", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "t_recv_ns", "field_name": "t_recv_ns", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "t_send_ns", "field_name": "t_send_ns", "pandas_type": "int64", "numpy_type": "Int64", "metadata": null}, {"name": "latency_ns", "field_name": "latency_ns", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "src_id", "field_name": "src_id", "pandas_type": "int32", "numpy_type": "int32", "metadata": null}, {"name": "dst_id", "field_name": "dst_id", "pandas_type": "int32", "numpy_type": "int32", "metadata": null}, {"name": "message_id", "field_name": "message_id", "pandas_type": "int64", "numpy_type": "int64", "metadata": null}, {"name": "msg_type", "field_name": "msg_type", "pandas_type": "unicode", "numpy_type": "string", "metadata": null}, {"name": "order_id", "field_name": "order_id", "pandas_type": "int64", "numpy_type": "Int64", "metadata": null}, {"name": "causal_parent", "field_name": "causal_parent", "pandas_type": "int64", "numpy_type": "Int64", "metadata": null}], "creator": {"library": "pyarrow", "version": "15.0.2"}, "pandas_version": "1.5.3"}"#;
const MSG_ARROW: &str = "/////6AHAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAADQFAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAAAIAAAAEAAAAAYAAABwYW5kYXMAAPwEAAB7ImluZGV4X2NvbHVtbnMiOiBbXSwgImNvbHVtbl9pbmRleGVzIjogW10sICJjb2x1bW5zIjogW3sibmFtZSI6ICJzZXEiLCAiZmllbGRfbmFtZSI6ICJzZXEiLCAicGFuZGFzX3R5cGUiOiAiaW50NjQiLCAibnVtcHlfdHlwZSI6ICJpbnQ2NCIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAidF9yZWN2X25zIiwgImZpZWxkX25hbWUiOiAidF9yZWN2X25zIiwgInBhbmRhc190eXBlIjogImludDY0IiwgIm51bXB5X3R5cGUiOiAiaW50NjQiLCAibWV0YWRhdGEiOiBudWxsfSwgeyJuYW1lIjogInRfc2VuZF9ucyIsICJmaWVsZF9uYW1lIjogInRfc2VuZF9ucyIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogIkludDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJsYXRlbmN5X25zIiwgImZpZWxkX25hbWUiOiAibGF0ZW5jeV9ucyIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogImludDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJzcmNfaWQiLCAiZmllbGRfbmFtZSI6ICJzcmNfaWQiLCAicGFuZGFzX3R5cGUiOiAiaW50MzIiLCAibnVtcHlfdHlwZSI6ICJpbnQzMiIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAiZHN0X2lkIiwgImZpZWxkX25hbWUiOiAiZHN0X2lkIiwgInBhbmRhc190eXBlIjogImludDMyIiwgIm51bXB5X3R5cGUiOiAiaW50MzIiLCAibWV0YWRhdGEiOiBudWxsfSwgeyJuYW1lIjogIm1lc3NhZ2VfaWQiLCAiZmllbGRfbmFtZSI6ICJtZXNzYWdlX2lkIiwgInBhbmRhc190eXBlIjogImludDY0IiwgIm51bXB5X3R5cGUiOiAiaW50NjQiLCAibWV0YWRhdGEiOiBudWxsfSwgeyJuYW1lIjogIm1zZ190eXBlIiwgImZpZWxkX25hbWUiOiAibXNnX3R5cGUiLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogInN0cmluZyIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAib3JkZXJfaWQiLCAiZmllbGRfbmFtZSI6ICJvcmRlcl9pZCIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogIkludDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJjYXVzYWxfcGFyZW50IiwgImZpZWxkX25hbWUiOiAiY2F1c2FsX3BhcmVudCIsICJwYW5kYXNfdHlwZSI6ICJpbnQ2NCIsICJudW1weV90eXBlIjogIkludDY0IiwgIm1ldGFkYXRhIjogbnVsbH1dLCAiY3JlYXRvciI6IHsibGlicmFyeSI6ICJweWFycm93IiwgInZlcnNpb24iOiAiMTUuMC4yIn0sICJwYW5kYXNfdmVyc2lvbiI6ICIxLjUuMyJ9AAAAAAoAAAAEAgAAvAEAAIQBAABMAQAAGAEAAOQAAACsAAAAeAAAAEAAAAAEAAAANP7//wAAAQIQAAAAIAAAAAQAAAAAAAAADQAAAGNhdXNhbF9wYXJlbnQAAAAw/v//AAAAAUAAAABs/v//AAABAhAAAAAcAAAABAAAAAAAAAAIAAAAb3JkZXJfaWQAAAAAZP7//wAAAAFAAAAAoP7//wAAAQUQAAAAIAAAAAQAAAAAAAAACAAAAG1zZ190eXBlAAAAAAQABAAEAAAA0P7//wAAAQIQAAAAHAAAAAQAAAAAAAAACgAAAG1lc3NhZ2VfaWQAAMj+//8AAAABQAAAAAT///8AAAECEAAAABgAAAAEAAAAAAAAAAYAAABkc3RfaWQAAPj+//8AAAABIAAAADT///8AAAECEAAAABgAAAAEAAAAAAAAAAYAAABzcmNfaWQAACj///8AAAABIAAAAGT///8AAAECEAAAABwAAAAEAAAAAAAAAAoAAABsYXRlbmN5X25zAABc////AAAAAUAAAACY////AAABAhAAAAAcAAAABAAAAAAAAAAJAAAAdF9zZW5kX25zAAAAkP///wAAAAFAAAAAzP///wAAAQIQAAAAHAAAAAQAAAAAAAAACQAAAHRfcmVjdl9ucwAAAMT///8AAAABQAAAABAAFAAIAAYABwAMAAAAEAAQAAAAAAABAhAAAAAcAAAABAAAAAAAAAADAAAAc2VxAAgADAAIAAcACAAAAAAAAAFAAAAAAAAAAA==";

/// One output column as a row accessor: `f(i)` is the value of row `i`.
/// `Dict` columns take values from a fixed table (`f` returns the table index): every
/// page then encodes on its own, so pages can be produced while the simulation still runs.
/// `Str`/`OptStr` build a first-appearance dictionary per column chunk (whole-chunk encoding only).
pub enum Col<'a> {
    I64(&'a (dyn Fn(usize) -> i64 + Sync)),
    I32(&'a (dyn Fn(usize) -> i32 + Sync)),
    OptI64(&'a (dyn Fn(usize) -> Option<i64> + Sync)),
    Str(&'a (dyn Fn(usize) -> &'static str + Sync)),
    OptStr(&'a (dyn Fn(usize) -> Option<&'static str> + Sync)),
    Dict(&'static [&'static str], &'a (dyn Fn(usize) -> u32 + Sync)),
}

fn err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

// ---- thrift compact + parquet primitive encoders --------------------------------------

fn uleb(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Thrift compact: field header with id delta 1 (all ids used here are consecutive or small jumps).
fn field(out: &mut Vec<u8>, last: &mut i16, id: i16, ty: u8) {
    let d = id - *last;
    assert!((1..=15).contains(&d));
    out.push(((d as u8) << 4) | ty);
    *last = id;
}

fn i32_field(out: &mut Vec<u8>, last: &mut i16, id: i16, v: i32) {
    field(out, last, id, 5);
    uleb(out, zigzag(v as i64));
}

const PAGE_DATA: i32 = 0;
const PAGE_DICT: i32 = 2;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const ENC_DELTA: i32 = 5;
const ENC_RLE_DICT: i32 = 8;

/// One column chunk under construction: dictionary page (optional) then data pages.
pub struct Chunk {
    buf: Vec<u8>,
    uncompressed: i64,
    first_data: Option<usize>,
    dict: bool,
    num_values: i64,
}

impl Chunk {
    fn new() -> Self {
        Chunk { buf: Vec::new(), uncompressed: 0, first_data: None, dict: false, num_values: 0 }
    }

    /// Appends `next`'s pages after this chunk's.
    fn append(&mut self, next: &Chunk) {
        if self.first_data.is_none() {
            self.first_data = next.first_data.map(|o| o + self.buf.len());
        }
        self.buf.extend_from_slice(&next.buf);
        self.uncompressed += next.uncompressed;
        self.dict |= next.dict;
        self.num_values += next.num_values;
    }

    /// Appends one uncompressed page (header + body).
    fn page(&mut self, kind: i32, num_values: usize, enc: i32, body: &[u8]) {
        let n = body.len();
        let mut h = Vec::with_capacity(32);
        let mut last = 0;
        i32_field(&mut h, &mut last, 1, kind);
        i32_field(&mut h, &mut last, 2, body.len() as i32);
        i32_field(&mut h, &mut last, 3, n as i32);
        if kind == PAGE_DATA {
            field(&mut h, &mut last, 5, 12); // data_page_header
            let mut l = 0;
            i32_field(&mut h, &mut l, 1, num_values as i32);
            i32_field(&mut h, &mut l, 2, enc);
            i32_field(&mut h, &mut l, 3, ENC_RLE);
            i32_field(&mut h, &mut l, 4, ENC_RLE);
            h.push(0);
            self.first_data.get_or_insert(self.buf.len());
            self.num_values += num_values as i64;
        } else {
            field(&mut h, &mut last, 7, 12); // dictionary_page_header
            let mut l = 0;
            i32_field(&mut h, &mut l, 1, num_values as i32);
            i32_field(&mut h, &mut l, 2, enc);
            h.push(0);
            self.dict = true;
        }
        h.push(0);
        self.uncompressed += (h.len() + body.len()) as i64;
        self.buf.reserve(h.len() + body.len());
        self.buf.extend_from_slice(&h);
        self.buf.extend_from_slice(body);
    }
}

/// v1 definition levels (max level 1) for `m` rows. `present` = None: all defined (one RLE run).
fn def_levels(out: &mut Vec<u8>, m: usize, present: Option<&[bool]>) {
    let len_at = out.len();
    out.extend_from_slice(&[0; 4]); // byte length, patched below
    match present {
        None => {
            uleb(out, (m as u64) << 1);
            out.push(1);
        }
        Some(p) => {
            uleb(out, (((m + 7) / 8) as u64) << 1 | 1);
            for g in p.chunks(8) {
                let mut b = 0u8;
                for (i, x) in g.iter().enumerate() {
                    b |= (*x as u8) << i;
                }
                out.push(b);
            }
        }
    }
    let len = (out.len() - len_at - 4) as u32;
    out[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
}

/// Appends `vals` bit-packed LSB-first at `width` bits each, zero-padded to a whole byte.
fn bitpack(out: &mut Vec<u8>, vals: impl Iterator<Item = u64>, width: u32) {
    if width == 0 {
        return;
    }
    let (mut acc, mut nb) = (0u128, 0u32);
    for v in vals {
        acc |= (v as u128) << nb;
        nb += width;
        while nb >= 64 {
            out.extend_from_slice(&(acc as u64).to_le_bytes());
            acc >>= 64;
            nb -= 64;
        }
    }
    let mut rest = nb.div_ceil(8);
    while rest > 0 {
        out.push(acc as u8);
        acc >>= 8;
        rest -= 1;
    }
}

const DELTA_BLOCK: usize = 128;
const DELTA_MINI: usize = 32;

/// DELTA_BINARY_PACKED; `is32` selects INT32 wrap-around semantics for the deltas.
fn delta_encode(out: &mut Vec<u8>, v: &[i64], is32: bool) {
    if is32 {
        delta_encode_as::<true>(out, v)
    } else {
        delta_encode_as::<false>(out, v)
    }
}

fn delta_encode_as<const IS32: bool>(out: &mut Vec<u8>, v: &[i64]) {
    uleb(out, DELTA_BLOCK as u64);
    uleb(out, (DELTA_BLOCK / DELTA_MINI) as u64);
    uleb(out, v.len() as u64);
    uleb(out, zigzag(v.first().copied().unwrap_or(0)));
    let mut deltas = [0i64; DELTA_BLOCK];
    let mut rel = [0u64; DELTA_BLOCK];
    let mut i = 1;
    while i < v.len() {
        let k = (v.len() - i).min(DELTA_BLOCK);
        for j in 0..k {
            deltas[j] = if IS32 { (v[i + j] as i32).wrapping_sub(v[i + j - 1] as i32) as i64 } else { v[i + j].wrapping_sub(v[i + j - 1]) };
        }
        let min = *deltas[..k].iter().min().unwrap();
        uleb(out, zigzag(min));
        for j in 0..DELTA_BLOCK {
            // A short final miniblock is padded with zeros (= min_delta).
            rel[j] = if j >= k {
                0
            } else if IS32 {
                (deltas[j] as i32).wrapping_sub(min as i32) as u32 as u64
            } else {
                deltas[j].wrapping_sub(min) as u64
            };
        }
        let used = k.div_ceil(DELTA_MINI);
        let mut widths = [0u8; DELTA_BLOCK / DELTA_MINI];
        for m in 0..used {
            // The OR has the same highest set bit as the max.
            let or = rel[m * DELTA_MINI..(m + 1) * DELTA_MINI].iter().fold(0u64, |a, x| a | x);
            widths[m] = (64 - or.leading_zeros()) as u8;
        }
        out.extend_from_slice(&widths);
        for m in 0..used {
            pack32(out, &rel[m * DELTA_MINI..(m + 1) * DELTA_MINI], widths[m] as u32);
        }
        i += k;
    }
}

/// Bit-packs 32 values at `width` bits each, LSB first: exactly 4 * width bytes.
fn pack32(out: &mut Vec<u8>, vals: &[u64], width: u32) {
    debug_assert_eq!(vals.len(), DELTA_MINI);
    if width == 0 {
        return;
    }
    let bytes = 4 * width as usize;
    let start = out.len();
    out.resize(start + bytes, 0);
    let dst = &mut out[start..];
    let (mut acc, mut nb, mut pos) = (0u128, 0u32, 0usize);
    for &x in vals {
        acc |= (x as u128) << nb;
        nb += width;
        if nb >= 64 {
            dst[pos..pos + 8].copy_from_slice(&(acc as u64).to_le_bytes());
            pos += 8;
            acc >>= 64;
            nb -= 64;
        }
    }
    let rest = bytes - pos; // 0 or 4: 4 * width bytes is a multiple of 4
    dst[pos..].copy_from_slice(&(acc as u64).to_le_bytes()[..rest]);
}

/// Dictionary-encodes strings; ids are assigned by first appearance.
struct Dict {
    entries: Vec<&'static str>,
    map: HashMap<&'static str, u32>,
    recent: Vec<(usize, usize, u32)>, // (ptr, len, id): skips hashing for repeated literals
}

impl Dict {
    fn new() -> Self {
        Dict { entries: Vec::new(), map: HashMap::new(), recent: Vec::new() }
    }
    fn id(&mut self, s: &'static str) -> u32 {
        let key = (s.as_ptr() as usize, s.len());
        for r in &self.recent {
            if (r.0, r.1) == key {
                return r.2;
            }
        }
        let next = self.entries.len() as u32;
        let id = *self.map.entry(s).or_insert(next);
        if id == next {
            self.entries.push(s);
        }
        if self.recent.len() < 16 {
            self.recent.push((key.0, key.1, id));
        }
        id
    }
}

fn encode_str_chunk(c: &mut Chunk, v: &[Option<&'static str>], all_present: bool) {
    let mut d = Dict::new();
    let ids: Vec<Option<u32>> = v.iter().map(|x| x.map(|s| d.id(s))).collect();
    let mut body = Vec::new();
    for s in &d.entries {
        body.extend_from_slice(&(s.len() as u32).to_le_bytes());
        body.extend_from_slice(s.as_bytes());
    }
    c.page(PAGE_DICT, d.entries.len(), ENC_PLAIN, &body);
    let width = (32 - (d.entries.len().max(2) as u32 - 1).leading_zeros()).max(1);
    let n = v.len();
    let mut s = 0;
    while s < n {
        let e = (s + PAGE_ROWS).min(n);
        body.clear();
        let present: Vec<bool> = ids[s..e].iter().map(|x| x.is_some()).collect();
        def_levels(&mut body, e - s, if all_present { None } else { Some(&present) });
        body.push(width as u8);
        let k = ids[s..e].iter().flatten().count();
        uleb(&mut body, ((k.div_ceil(8)) as u64) << 1 | 1);
        bitpack(&mut body, ids[s..e].iter().flatten().map(|x| *x as u64), width);
        c.page(PAGE_DATA, e - s, ENC_RLE_DICT, &body);
        s = e;
    }
}

fn encode_col(col: &Col, start: usize, end: usize) -> Chunk {
    let mut c = match col {
        Col::Str(f) => {
            let o: Vec<Option<&'static str>> = (start..end).map(|i| Some(f(i))).collect();
            let mut c = Chunk::new();
            encode_str_chunk(&mut c, &o, true);
            return c;
        }
        Col::OptStr(f) => {
            let o: Vec<Option<&'static str>> = (start..end).map(|i| f(i)).collect();
            let mut c = Chunk::new();
            encode_str_chunk(&mut c, &o, false);
            return c;
        }
        Col::Dict(t, _) => dict_chunk(t),
        _ => Chunk::new(),
    };
    let mut s = start;
    while s < end {
        let e = (s + PAGE_ROWS).min(end);
        c.append(&encode_page(col, s, e));
        s = e;
    }
    c
}

/// Rows per data page; a page-level producer must cut pages at multiples of this.
pub const PAGE: usize = PAGE_ROWS;

/// The dictionary page of a fixed-table column (PLAIN entries).
fn dict_chunk(table: &[&'static str]) -> Chunk {
    let mut c = Chunk::new();
    let mut body = Vec::new();
    for s in table {
        body.extend_from_slice(&(s.len() as u32).to_le_bytes());
        body.extend_from_slice(s.as_bytes());
    }
    c.page(PAGE_DICT, table.len(), ENC_PLAIN, &body);
    c
}

/// One data page of rows `start..end` (at most PAGE rows) as a chunk fragment.
/// Supports the page-independent column kinds (ints and fixed-table strings).
pub fn encode_page(col: &Col, start: usize, end: usize) -> Chunk {
    debug_assert!(end - start <= PAGE_ROWS);
    // Per-thread buffers: a fresh allocation per page faults in (and zeroes) new memory each time.
    thread_local! {
        static BUF: RefCell<(Vec<i64>, Vec<bool>, Vec<u32>, Vec<u8>)> = RefCell::default();
    }
    BUF.with(|b| {
        let (vals, present, ids, body) = &mut *b.borrow_mut();
        vals.clear();
        present.clear();
        ids.clear();
        body.clear();
        let n = end - start;
        let mut c = Chunk::new();
        match col {
            Col::I64(f) => {
                vals.extend((start..end).map(|i| f(i)));
                def_levels(body, n, None);
                delta_encode(body, vals, false);
            }
            Col::I32(f) => {
                vals.extend((start..end).map(|i| f(i) as i64));
                def_levels(body, n, None);
                delta_encode(body, vals, true);
            }
            Col::OptI64(f) => {
                for i in start..end {
                    let v = f(i);
                    present.push(v.is_some());
                    if let Some(v) = v {
                        vals.push(v);
                    }
                }
                def_levels(body, n, Some(present));
                delta_encode(body, vals, false);
            }
            Col::Dict(t, f) => {
                ids.extend((start..end).map(|i| f(i)));
                dict_data_page(&mut c, t.len(), n, ids);
                return c;
            }
            Col::Str(_) | Col::OptStr(_) => panic!("first-appearance dictionaries are chunk-level only"),
        }
        c.page(PAGE_DATA, n, ENC_DELTA, body);
        c
    })
}

/// RLE_DICTIONARY data page (all rows present) over a dictionary of `dict_len` entries.
fn dict_data_page(c: &mut Chunk, dict_len: usize, rows: usize, ids: &[u32]) {
    let width = (32 - (dict_len.max(2) as u32 - 1).leading_zeros()).max(1);
    let mut body = Vec::with_capacity(16 + ids.len());
    def_levels(&mut body, rows, None);
    body.push(width as u8);
    uleb(&mut body, ((ids.len().div_ceil(8)) as u64) << 1 | 1);
    bitpack(&mut body, ids.iter().map(|x| *x as u64), width);
    c.page(PAGE_DATA, rows, ENC_RLE_DICT, &body);
}

/// Assembles a file from per-column page fragments: `pages[col][page]`, where page `p` holds
/// `page_rows[p]` rows in every column. Consecutive pages form row groups of at most ROW_GROUP
/// rows (at least one page each); `dicts[col]` is the fixed table of a dictionary column.
/// Returns the SHA-256.
fn assemble_pages(
    path: &Path,
    schema: &str,
    pandas_json: &str,
    arrow_schema_b64: &str,
    dicts: &[Option<&'static [&'static str]>],
    pages: Vec<Vec<Chunk>>,
    page_rows: &[usize],
) -> io::Result<String> {
    let mut groups: Vec<(usize, usize)> = Vec::new(); // (rows, pages)
    let (mut rows, mut np) = (0, 0);
    for &r in page_rows {
        if np > 0 && rows + r > ROW_GROUP {
            groups.push((rows, np));
            (rows, np) = (0, 0);
        }
        rows += r;
        np += 1;
    }
    if np > 0 {
        groups.push((rows, np));
    }
    let mut cols: Vec<_> = pages.into_iter().map(Vec::into_iter).collect();
    let mut chunks = Vec::with_capacity(groups.len() * dicts.len());
    for &(_, np) in &groups {
        for (c, d) in dicts.iter().enumerate() {
            let mut chunk = match d {
                Some(t) => dict_chunk(t),
                None => Chunk::new(),
            };
            for pg in cols[c].by_ref().take(np) {
                chunk.append(&pg);
            }
            chunks.push(chunk);
        }
    }
    let rg_rows: Vec<usize> = groups.iter().map(|g| g.0).collect();
    assemble(path, schema, pandas_json, arrow_schema_b64, &rg_rows, chunks)
}

pub fn assemble_trace_pages(path: &Path, dicts: &[Option<&'static [&'static str]>; 7], pages: Vec<Vec<Chunk>>, page_rows: &[usize]) -> io::Result<String> {
    assemble_pages(path, TRACE_SCHEMA, TRACE_PANDAS, TRACE_ARROW, dicts, pages, page_rows)
}

pub fn assemble_message_pages(path: &Path, dicts: &[Option<&'static [&'static str]>; 10], pages: Vec<Vec<Chunk>>, page_rows: &[usize]) -> io::Result<String> {
    assemble_pages(path, MSG_SCHEMA, MSG_PANDAS, MSG_ARROW, dicts, pages, page_rows)
}

/// Number of independent encode jobs for `n` rows of `ncols` columns: one per (row group, column).
pub fn n_jobs(n: usize, ncols: usize) -> usize {
    n.div_ceil(ROW_GROUP) * ncols
}

/// Encodes job `j` (row group `j / cols.len()`, column `j % cols.len()`).
pub fn encode_job(cols: &[Col], n: usize, j: usize) -> Chunk {
    let (rg, c) = (j / cols.len(), j % cols.len());
    encode_col(&cols[c], rg * ROW_GROUP, ((rg + 1) * ROW_GROUP).min(n))
}

/// Encodes every (row group, column) chunk, on `threads` scoped workers when `threads > 1`.
fn encode_all(cols: &[Col], n: usize, threads: usize) -> Vec<Chunk> {
    let jobs = n_jobs(n, cols.len());
    let job = |j: usize| encode_job(cols, n, j);
    if threads <= 1 || jobs <= 1 {
        return (0..jobs).map(|j| job(j)).collect();
    }
    let next = AtomicUsize::new(0);
    let out: Vec<Mutex<Option<Chunk>>> = (0..jobs).map(|_| Mutex::new(None)).collect();
    std::thread::scope(|s| {
        for _ in 0..threads.min(jobs) {
            s.spawn(|| {
                        loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= jobs {
                        break;
                    }
                    *out[j].lock().unwrap() = Some(job(j));
                }
            });
        }
    });
    out.into_iter().map(|m| m.into_inner().unwrap().expect("encoded chunk")).collect()
}

/// Writes `n` rows of `cols` (schema order) to `path`; returns the file's SHA-256 (hex).
fn write_file(
    path: &Path,
    schema: &str,
    pandas_json: &str,
    arrow_schema_b64: &str,
    n: usize,
    cols: &[Col],
    threads: usize,
) -> io::Result<String> {
    let chunks = encode_all(cols, n, threads);
    let rg_rows: Vec<usize> = (0..n).step_by(ROW_GROUP).map(|s| (n - s).min(ROW_GROUP)).collect();
    assemble(path, schema, pandas_json, arrow_schema_b64, &rg_rows, chunks)
}

/// Serializes already-encoded chunks (row group by row group, columns in schema order) into a
/// parquet file at `path`, row group `g` holding `rg_rows[g]` rows; returns its SHA-256.
fn assemble(
    path: &Path,
    schema: &str,
    pandas_json: &str,
    arrow_schema_b64: &str,
    rg_rows: &[usize],
    chunks: Vec<Chunk>,
) -> io::Result<String> {
    let schema = Arc::new(parse_message_type(schema).map_err(err)?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_key_value_metadata(Some(vec![
                KeyValue::new("ARROW:schema".to_string(), arrow_schema_b64.to_string()),
                KeyValue::new("pandas".to_string(), pandas_json.to_string()),
            ]))
            .build(),
    );
    let mut chunks = chunks.into_iter();
    let mut file: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut w = SerializedFileWriter::new(&mut file, schema, props).map_err(err)?;
    let descrs: Vec<_> = (0..w.schema_descr().num_columns()).map(|i| w.schema_descr().column(i)).collect();
    for &rows in rg_rows {
        let mut rg = w.next_row_group().map_err(err)?;
        for d in &descrs {
            let c = chunks.next().expect("chunk per column");
            let enc = if c.dict {
                vec![Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY]
            } else {
                vec![Encoding::RLE, Encoding::DELTA_BINARY_PACKED]
            };
            let meta = ColumnChunkMetaData::builder(d.clone())
                .set_compression(Compression::UNCOMPRESSED)
                .set_encodings(enc)
                .set_num_values(c.num_values)
                .set_total_compressed_size(c.buf.len() as i64)
                .set_total_uncompressed_size(c.uncompressed)
                .set_data_page_offset(c.first_data.expect("data page") as i64)
                .set_dictionary_page_offset(if c.dict { Some(0) } else { None })
                .build()
                .map_err(err)?;
            let close = ColumnCloseResult {
                bytes_written: c.buf.len() as u64,
                rows_written: rows as u64,
                metadata: meta,
                bloom_filter: None,
                column_index: None,
                offset_index: None,
            };
            rg.append_column(&Bytes::from(c.buf), close).map_err(err)?;
        }
        rg.close().map_err(err)?;
    }
    w.close().map_err(err)?;
    // Hash and write the same bytes concurrently: both are single-threaded passes over the file.
    let (written, sha) = std::thread::scope(|sc| {
        let h = sc.spawn(|| hex(&Sha256::digest(&file)));
        (std::fs::write(path, &file), h.join().expect("hash thread panicked"))
    });
    written?;
    Ok(sha)
}

fn hex(d: &[u8]) -> String {
    d.iter().map(|b| format!("{:02x}", b)).collect()
}

/// trace.parquet; `cols` in schema order: t_ns, agent_id, msg_type, side, price, size, order_id.
pub fn write_trace_cols(path: &Path, n: usize, cols: &[Col; 7], threads: usize) -> io::Result<String> {
    write_file(path, TRACE_SCHEMA, TRACE_PANDAS, TRACE_ARROW, n, cols, threads)
}

/// message_trace.parquet; `cols` in schema order: seq, t_recv_ns, t_send_ns, latency_ns, src_id,
/// dst_id, message_id, msg_type, order_id, causal_parent.
pub fn write_message_cols(path: &Path, n: usize, cols: &[Col; 10], threads: usize) -> io::Result<String> {
    write_file(path, MSG_SCHEMA, MSG_PANDAS, MSG_ARROW, n, cols, threads)
}

pub fn write_trace(path: &Path, t: &TraceCols) -> io::Result<()> {
    write_trace_cols(
        path,
        t.t_ns.len(),
        &[
            Col::I64(&|i| t.t_ns[i]),
            Col::I32(&|i| t.agent_id[i]),
            Col::Str(&|i| t.msg_type[i]),
            Col::OptStr(&|i| t.side[i]),
            Col::I64(&|i| t.price[i]),
            Col::I64(&|i| t.size[i]),
            Col::I64(&|i| t.order_id[i]),
        ],
        1,
    )
    .map(|_| ())
}

pub fn write_message_trace(path: &Path, m: &MsgCols) -> io::Result<()> {
    write_message_cols(
        path,
        m.seq.len(),
        &[
            Col::I64(&|i| m.seq[i]),
            Col::I64(&|i| m.t_recv_ns[i]),
            Col::OptI64(&|i| m.t_send_ns[i]),
            Col::I64(&|i| m.latency_ns[i]),
            Col::I32(&|i| m.src_id[i]),
            Col::I32(&|i| m.dst_id[i]),
            Col::I64(&|i| m.message_id[i]),
            Col::Str(&|i| m.msg_type[i]),
            Col::OptI64(&|i| m.order_id[i]),
            Col::OptI64(&|i| m.causal_parent[i]),
        ],
        1,
    )
    .map(|_| ())
}

pub fn sha256_file(path: &Path) -> io::Result<String> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let k = f.read(&mut buf)?;
        if k == 0 {
            break;
        }
        h.update(&buf[..k]);
    }
    Ok(hex(&h.finalize()))
}

