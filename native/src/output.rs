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
//! Uses the low-level parquet column writers (no arrow crate): 1M rows << 100 ms.

use std::fs::File;
use std::io::{self, BufWriter, Read};
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnPath;
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

// Columns that are low-cardinality get dictionary encoding (as pyarrow does);
// the rest are written PLAIN (dictionary on unique-ish ids only costs time).
const TRACE_DICT: &[&str] = &["agent_id", "msg_type", "side"];
const MSG_DICT: &[&str] = &["src_id", "dst_id", "msg_type"];

enum Col<'a> {
    I64(&'a [i64]),
    I32(&'a [i32]),
    OptI64(&'a [Option<i64>]),
    Str(&'a [&'static str]),
    OptStr(&'a [Option<&'static str>]),
}

impl Col<'_> {
    fn len(&self) -> usize {
        match self {
            Col::I64(v) => v.len(),
            Col::I32(v) => v.len(),
            Col::OptI64(v) => v.len(),
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
        }
    }
}

fn err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

fn ba(s: &'static str) -> ByteArray {
    // Static bytes: no allocation, no copy.
    ByteArray::from(bytes::Bytes::from_static(s.as_bytes()))
}

fn write_file(
    path: &Path,
    schema: &str,
    dict_cols: &[&str],
    names: &[&str],
    pandas_json: &str,
    arrow_schema_b64: &str,
    cols: &[Col],
) -> io::Result<()> {
    let n = cols[0].len();
    assert!(cols.iter().all(|c| c.len() == n), "column length mismatch");
    assert_eq!(names.len(), cols.len());
    let schema = Arc::new(parse_message_type(schema).map_err(err)?);
    let mut pb = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(ROW_GROUP))
        .set_key_value_metadata(Some(vec![
            KeyValue::new("ARROW:schema".to_string(), arrow_schema_b64.to_string()),
            KeyValue::new("pandas".to_string(), pandas_json.to_string()),
        ]));
    for d in dict_cols {
        pb = pb.set_column_dictionary_enabled(ColumnPath::from(*d), true);
    }
    let props = Arc::new(pb.build());
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props).map_err(err)?;

    let ones = vec![1i16; ROW_GROUP.min(n)];
    let mut start = 0;
    while start < n {
        let end = (start + ROW_GROUP).min(n);
        let m = end - start;
        let mut rg = w.next_row_group().map_err(err)?;
        for col in cols {
            let mut cw = rg.next_column().map_err(err)?.expect("column count");
            match col {
                Col::I64(v) => {
                    cw.typed::<Int64Type>().write_batch(&v[start..end], Some(&ones[..m]), None).map_err(err)?;
                }
                Col::I32(v) => {
                    cw.typed::<Int32Type>().write_batch(&v[start..end], Some(&ones[..m]), None).map_err(err)?;
                }
                Col::OptI64(v) => {
                    let mut vals = Vec::with_capacity(m);
                    let mut defs = Vec::with_capacity(m);
                    for x in &v[start..end] {
                        defs.push(x.is_some() as i16);
                        if let Some(x) = x {
                            vals.push(*x);
                        }
                    }
                    cw.typed::<Int64Type>().write_batch(&vals, Some(&defs), None).map_err(err)?;
                }
                Col::Str(v) => {
                    let vals: Vec<ByteArray> = v[start..end].iter().map(|s| ba(s)).collect();
                    cw.typed::<ByteArrayType>().write_batch(&vals, Some(&ones[..m]), None).map_err(err)?;
                }
                Col::OptStr(v) => {
                    let mut vals = Vec::with_capacity(m);
                    let mut defs = Vec::with_capacity(m);
                    for x in &v[start..end] {
                        defs.push(x.is_some() as i16);
                        if let Some(x) = x {
                            vals.push(ba(x));
                        }
                    }
                    cw.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).map_err(err)?;
                }
            }
            cw.close().map_err(err)?;
        }
        rg.close().map_err(err)?;
        start = end;
    }
    w.close().map_err(err)?;
    Ok(())
}

pub fn write_trace(path: &Path, t: &TraceCols) -> io::Result<()> {
    write_file(
        path,
        TRACE_SCHEMA,
        TRACE_DICT,
        &["t_ns", "agent_id", "msg_type", "side", "price", "size", "order_id"],
        TRACE_PANDAS,
        TRACE_ARROW,
        &[
            Col::I64(&t.t_ns),
            Col::I32(&t.agent_id),
            Col::Str(&t.msg_type),
            Col::OptStr(&t.side),
            Col::I64(&t.price),
            Col::I64(&t.size),
            Col::I64(&t.order_id),
        ],
    )
}

pub fn write_message_trace(path: &Path, m: &MsgCols) -> io::Result<()> {
    write_file(
        path,
        MSG_SCHEMA,
        MSG_DICT,
        &["seq", "t_recv_ns", "t_send_ns", "latency_ns", "src_id", "dst_id", "message_id", "msg_type", "order_id", "causal_parent"],
        MSG_PANDAS,
        MSG_ARROW,
        &[
            Col::I64(&m.seq),
            Col::I64(&m.t_recv_ns),
            Col::OptI64(&m.t_send_ns),
            Col::I64(&m.latency_ns),
            Col::I32(&m.src_id),
            Col::I32(&m.dst_id),
            Col::I64(&m.message_id),
            Col::Str(&m.msg_type),
            Col::OptI64(&m.order_id),
            Col::OptI64(&m.causal_parent),
        ],
    )
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
    Ok(h.finalize().iter().map(|b| format!("{:02x}", b)).collect())
}

