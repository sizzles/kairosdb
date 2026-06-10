//! Cassandra-backed `Datastore` implementation over the `scylla` CQL driver
//! (which speaks to both Cassandra and ScyllaDB).
//!
//! Schema, statements, and byte formats mirror the Java implementation
//! (`ClusterConnection`, `CQLBatch`, `CQLFilteredRowKeyIterator`): the same
//! `data_points` / `row_keys` / `row_key_time_index` / `string_index` tables,
//! the same row-key blobs (see [`crate::cassandra`]), and the same 4-byte
//! big-endian column-time encoding — so this backend can run against a
//! keyspace populated by a Java KairosDB node, and vice versa.

use std::collections::{BTreeMap, HashMap};

use futures::stream::{self, StreamExt, TryStreamExt};
use kairos_core::value::DST_LEGACY;
use kairos_core::{DataPoint, DataPointSet, Tags, Value};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use crate::cassandra::{DataPointsRowKey, RowSpec, RowUnit, DEFAULT_ROW_WIDTH_MS};
use crate::{tags_match, Datastore, DatastoreQuery, Error, Result, SeriesData};

const DATA_POINTS_TABLE: &str = "data_points";

// String index partition keys, from CassandraDatastore.
const ROW_KEY_METRIC_NAMES: &str = "metric_names";
const ROW_KEY_TAG_NAMES: &str = "tag_names";
const ROW_KEY_TAG_VALUES: &str = "tag_values";

// DDL identical in shape to ClusterConnection's CREATE TABLE statements.
const TABLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS data_points (
        key blob, column1 blob, value blob,
        PRIMARY KEY ((key), column1))",
    "CREATE TABLE IF NOT EXISTS row_keys (
        metric text, table_name text, row_time timestamp,
        data_type text, tags frozen<map<text, text>>,
        mtime timeuuid static, value text,
        PRIMARY KEY ((metric, table_name, row_time), data_type, tags))",
    "CREATE TABLE IF NOT EXISTS row_key_time_index (
        metric text, table_name text, row_time timestamp, value text,
        PRIMARY KEY ((metric), table_name, row_time))",
    "CREATE TABLE IF NOT EXISTS string_index (
        key blob, column1 text, value blob,
        PRIMARY KEY ((key), column1))",
    "CREATE TABLE IF NOT EXISTS spec (
        spec_type text, name text, value text,
        PRIMARY KEY ((spec_type), name))",
];

pub struct CassandraConfig {
    pub node: String,
    pub keyspace: String,
    pub replication: String,
}

impl Default for CassandraConfig {
    fn default() -> Self {
        CassandraConfig {
            node: "127.0.0.1:9042".to_string(),
            keyspace: "kairosdb".to_string(),
            replication: "{'class': 'SimpleStrategy', 'replication_factor': 1}".to_string(),
        }
    }
}

pub struct CassandraDatastore {
    session: Session,
    spec: RowSpec,
    ps_data_point_insert: PreparedStatement,
    ps_row_key_insert: PreparedStatement,
    ps_row_key_time_insert: PreparedStatement,
    ps_string_index_insert: PreparedStatement,
    ps_row_key_time_query: PreparedStatement,
    ps_row_key_query: PreparedStatement,
    ps_data_points_query: PreparedStatement,
    ps_data_points_delete_range: PreparedStatement,
    ps_string_index_query: PreparedStatement,
}

fn store_err(context: &str, e: impl std::fmt::Display) -> Error {
    Error::Datastore(format!("{context}: {e}"))
}

async fn read_spec_value(session: &Session, name: &str) -> Result<Option<String>> {
    let result = session
        .query_unpaged(
            "SELECT value FROM spec WHERE spec_type = 'cluster_config' AND name = ?",
            (name,),
        )
        .await
        .map_err(|e| store_err("spec query", e))?
        .into_rows_result()
        .map_err(|e| store_err("spec rows", e))?;
    let mut rows = result
        .rows::<(String,)>()
        .map_err(|e| store_err("spec decode", e))?;
    match rows.next() {
        Some(row) => Ok(Some(row.map_err(|e| store_err("spec row", e))?.0)),
        None => Ok(None),
    }
}

async fn write_spec_value(session: &Session, name: &str, value: &str) -> Result<()> {
    session
        .query_unpaged(
            "INSERT INTO spec (spec_type, name, value) VALUES ('cluster_config', ?, ?)",
            (name, value),
        )
        .await
        .map_err(|e| store_err("spec insert", e))?;
    Ok(())
}

/// Determine the cluster's row format the way `ClusterConnection` does:
/// honor the `spec` table when present; otherwise an existing cluster
/// (metric names already indexed) is legacy, and a fresh cluster gets the
/// modern millisecond format recorded into `spec`.
async fn negotiate_row_spec(session: &Session) -> Result<RowSpec> {
    let mut spec = RowSpec {
        row_width: DEFAULT_ROW_WIDTH_MS,
        unit: RowUnit::Milliseconds,
        legacy: false,
    };

    match read_spec_value(session, "row_time_unit").await? {
        Some(unit) => match unit.as_str() {
            "LEGACY" => spec.legacy = true,
            "MILLISECONDS" => spec.unit = RowUnit::Milliseconds,
            "SECONDS" => spec.unit = RowUnit::Seconds,
            other => {
                return Err(Error::Datastore(format!(
                    "unsupported row_time_unit in spec table: {other}"
                )))
            }
        },
        None => {
            let has_data = session
                .query_unpaged(
                    "SELECT column1 FROM string_index WHERE key = ? LIMIT 1",
                    (ROW_KEY_METRIC_NAMES.as_bytes().to_vec(),),
                )
                .await
                .map_err(|e| store_err("string_index probe", e))?
                .into_rows_result()
                .map_err(|e| store_err("string_index probe rows", e))?
                .rows_num()
                > 0;
            if has_data {
                spec.legacy = true;
                write_spec_value(session, "row_time_unit", "LEGACY").await?;
            } else {
                write_spec_value(session, "row_time_unit", "MILLISECONDS").await?;
            }
        }
    }

    match read_spec_value(session, "row_width").await? {
        Some(width) => {
            spec.row_width = width
                .parse()
                .map_err(|e| store_err("row_width parse", e))?;
        }
        None => {
            write_spec_value(session, "row_width", &spec.row_width.to_string()).await?;
        }
    }

    Ok(spec)
}

impl CassandraDatastore {
    pub async fn connect(config: &CassandraConfig) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_node(&config.node)
            .build()
            .await
            .map_err(|e| store_err("connect", e))?;

        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {}",
                    config.keyspace, config.replication
                ),
                (),
            )
            .await
            .map_err(|e| store_err("create keyspace", e))?;
        session
            .use_keyspace(&config.keyspace, false)
            .await
            .map_err(|e| store_err("use keyspace", e))?;
        for ddl in TABLES {
            session
                .query_unpaged(*ddl, ())
                .await
                .map_err(|e| store_err("create table", e))?;
        }

        let spec = negotiate_row_spec(&session).await?;

        let prepare = |stmt: &'static str| {
            let session = &session;
            async move { session.prepare(stmt).await.map_err(|e| store_err(stmt, e)) }
        };

        Ok(CassandraDatastore {
            // Statements match ClusterConnection's prepared statements.
            ps_data_point_insert: prepare(
                "INSERT INTO data_points (key, column1, value) VALUES (?, ?, ?) \
                 USING TTL ? AND TIMESTAMP ?",
            )
            .await?,
            ps_row_key_insert: prepare(
                "INSERT INTO row_keys (metric, table_name, row_time, data_type, tags, mtime) \
                 VALUES (?, ?, ?, ?, ?, now()) USING TTL ?",
            )
            .await?,
            ps_row_key_time_insert: prepare(
                "INSERT INTO row_key_time_index (metric, table_name, row_time) \
                 VALUES (?, ?, ?) USING TTL ?",
            )
            .await?,
            ps_string_index_insert: prepare(
                "INSERT INTO string_index (key, column1, value) VALUES (?, ?, 0x00)",
            )
            .await?,
            ps_row_key_time_query: prepare(
                "SELECT row_time FROM row_key_time_index \
                 WHERE metric = ? AND table_name = ? AND row_time >= ? AND row_time <= ?",
            )
            .await?,
            ps_row_key_query: prepare(
                "SELECT row_time, data_type, tags FROM row_keys \
                 WHERE metric = ? AND table_name = ? AND row_time = ?",
            )
            .await?,
            ps_data_points_query: prepare(
                "SELECT column1, value FROM data_points \
                 WHERE key = ? AND column1 >= ? AND column1 < ? ORDER BY column1 ASC",
            )
            .await?,
            ps_data_points_delete_range: prepare(
                "DELETE FROM data_points WHERE key = ? AND column1 >= ? AND column1 <= ?",
            )
            .await?,
            ps_string_index_query: prepare("SELECT column1 FROM string_index WHERE key = ?")
                .await?,
            session,
            spec,
        })
    }

    /// Row keys outlive their points by one row width, as in the Java write
    /// path; a TTL of 0 means never expire.
    fn row_key_ttl(&self, point_ttl: u32) -> i32 {
        if point_ttl == 0 {
            0
        } else {
            point_ttl as i32 + (self.spec.row_width_ms() / 1000) as i32
        }
    }

    async fn index_string(&self, index_key: &str, value: &str) -> Result<()> {
        self.session
            .execute_unpaged(
                &self.ps_string_index_insert,
                (index_key.as_bytes().to_vec(), value),
            )
            .await
            .map_err(|e| store_err("string_index insert", e))?;
        Ok(())
    }

    async fn query_string_index(&self, index_key: &str) -> Result<Vec<String>> {
        let result = self
            .session
            .execute_unpaged(&self.ps_string_index_query, (index_key.as_bytes().to_vec(),))
            .await
            .map_err(|e| store_err("string_index query", e))?
            .into_rows_result()
            .map_err(|e| store_err("string_index rows", e))?;
        let mut names = Vec::new();
        for row in result
            .rows::<(String,)>()
            .map_err(|e| store_err("string_index decode", e))?
        {
            names.push(row.map_err(|e| store_err("string_index row", e))?.0);
        }
        Ok(names)
    }

    /// Row keys matching the query, via `row_key_time_index` then `row_keys`,
    /// filtered by data type and tags like `CQLFilteredRowKeyIterator`. The
    /// per-window `row_keys` lookups run concurrently, as the Java driver's
    /// async futures do.
    async fn matching_row_keys(&self, query: &DatastoreQuery) -> Result<Vec<DataPointsRowKey>> {
        let start_row_time = self.spec.calculate_row_time(query.start_time_ms);
        let times = self
            .session
            .execute_unpaged(
                &self.ps_row_key_time_query,
                (
                    query.metric.as_str(),
                    DATA_POINTS_TABLE,
                    CqlTimestamp(start_row_time),
                    CqlTimestamp(query.end_time_ms),
                ),
            )
            .await
            .map_err(|e| store_err("row_key_time_index query", e))?
            .into_rows_result()
            .map_err(|e| store_err("row_key_time_index rows", e))?;

        let mut row_times = Vec::new();
        for time_row in times
            .rows::<(CqlTimestamp,)>()
            .map_err(|e| store_err("row_key_time_index decode", e))?
        {
            row_times.push(time_row.map_err(|e| store_err("row_key_time_index row", e))?.0);
        }

        let key_batches: Vec<Vec<DataPointsRowKey>> =
            stream::iter(row_times.into_iter().map(|row_time| async move {
                let keys = self
                    .session
                    .execute_unpaged(
                        &self.ps_row_key_query,
                        (query.metric.as_str(), DATA_POINTS_TABLE, row_time),
                    )
                    .await
                    .map_err(|e| store_err("row_keys query", e))?
                    .into_rows_result()
                    .map_err(|e| store_err("row_keys rows", e))?;
                let mut row_keys = Vec::new();
                for key_row in keys
                    .rows::<(CqlTimestamp, String, HashMap<String, String>)>()
                    .map_err(|e| store_err("row_keys decode", e))?
                {
                    let (row_time, data_type, tags) =
                        key_row.map_err(|e| store_err("row_keys row", e))?;
                    let tags: Tags = tags.into_iter().collect();
                    if tags_match(&tags, &query.tags) {
                        row_keys.push(DataPointsRowKey {
                            metric_name: query.metric.clone(),
                            row_time_ms: row_time.0,
                            data_type,
                            tags,
                        });
                    }
                }
                Ok::<_, Error>(row_keys)
            }))
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(key_batches.into_iter().flatten().collect())
    }

    /// Column-name bounds for one row, matching `CassandraDatastore`: clamp
    /// to the row when the query window extends past it, and add 1 to the end
    /// so the final column is included.
    fn column_range(&self, row_time: i64, query: &DatastoreQuery) -> (i32, i32) {
        let start = if query.start_time_ms < row_time {
            0
        } else {
            self.spec.column_name(row_time, query.start_time_ms)
        };
        let row_end = row_time + self.spec.row_width_ms();
        let end = if query.end_time_ms >= row_end {
            self.spec.column_name(row_time, row_end) + 1
        } else {
            self.spec.column_name(row_time, query.end_time_ms) + 1
        };
        (start, end)
    }

    async fn read_row(
        &self,
        row_key: &DataPointsRowKey,
        query: &DatastoreQuery,
    ) -> Result<Vec<DataPoint>> {
        let (start_col, end_col) = self.column_range(row_key.row_time_ms, query);
        let result = self
            .session
            .execute_unpaged(
                &self.ps_data_points_query,
                (
                    row_key.to_bytes(),
                    start_col.to_be_bytes().to_vec(),
                    end_col.to_be_bytes().to_vec(),
                ),
            )
            .await
            .map_err(|e| store_err("data_points query", e))?
            .into_rows_result()
            .map_err(|e| store_err("data_points rows", e))?;

        let mut points = Vec::new();
        for row in result
            .rows::<(Vec<u8>, Vec<u8>)>()
            .map_err(|e| store_err("data_points decode", e))?
        {
            let (column, value) = row.map_err(|e| store_err("data_points row", e))?;
            let column: [u8; 4] = column
                .try_into()
                .map_err(|_| Error::Datastore("data_points column1 is not 4 bytes".into()))?;
            let timestamp_ms = self
                .spec
                .column_timestamp(row_key.row_time_ms, i32::from_be_bytes(column));
            points.push(DataPoint {
                timestamp_ms,
                value: Value::read_from(&row_key.data_type, &value)?,
            });
        }
        Ok(points)
    }
}

/// Cassandra rejects oversized batches; chunk like the Java `CQLBatch`
/// host-partitioned batching does.
const WRITE_BATCH_SIZE: usize = 64;

/// In-flight read fan-out, the analogue of the Java query semaphore.
const READ_CONCURRENCY: usize = 16;

impl Datastore for CassandraDatastore {
    async fn write(&self, set: DataPointSet) -> Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_millis() as i64;
        let point_ttl = set.ttl as i32;
        let row_key_ttl = self.row_key_ttl(set.ttl);
        let cql_tags: HashMap<String, String> = set.tags.clone().into_iter().collect();

        // Data point inserts go out in unlogged batches; the row-key index
        // writes below are amortized to one per (row window, data type).
        let mut point_rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, i32, i64)> =
            Vec::with_capacity(set.points.len());
        let mut indexed_rows: BTreeMap<i64, Vec<String>> = BTreeMap::new();
        for point in &set.points {
            let row_time = self.spec.calculate_row_time(point.timestamp_ms);
            let row_key = DataPointsRowKey {
                metric_name: set.name.clone(),
                row_time_ms: row_time,
                data_type: point.value.datastore_type().to_string(),
                tags: set.tags.clone(),
            };

            let column = self.spec.column_name(row_time, point.timestamp_ms);
            let mut value_bytes = Vec::new();
            point.value.write_to(&mut value_bytes);
            point_rows.push((
                row_key.to_bytes(),
                column.to_be_bytes().to_vec(),
                value_bytes,
                point_ttl,
                now_ms,
            ));

            // Index each (row_time, data_type) once per write call.
            let types = indexed_rows.entry(row_time).or_default();
            if !types.contains(&row_key.data_type) {
                self.session
                    .execute_unpaged(
                        &self.ps_row_key_insert,
                        (
                            set.name.as_str(),
                            DATA_POINTS_TABLE,
                            CqlTimestamp(row_time),
                            row_key.data_type.as_str(),
                            &cql_tags,
                            row_key_ttl,
                        ),
                    )
                    .await
                    .map_err(|e| store_err("row_keys insert", e))?;
                self.session
                    .execute_unpaged(
                        &self.ps_row_key_time_insert,
                        (
                            set.name.as_str(),
                            DATA_POINTS_TABLE,
                            CqlTimestamp(row_time),
                            row_key_ttl,
                        ),
                    )
                    .await
                    .map_err(|e| store_err("row_key_time_index insert", e))?;
                types.push(row_key.data_type);
            }
        }

        for chunk in point_rows.chunks(WRITE_BATCH_SIZE) {
            if let [single] = chunk {
                self.session
                    .execute_unpaged(&self.ps_data_point_insert, single)
                    .await
                    .map_err(|e| store_err("data_points insert", e))?;
                continue;
            }
            let mut batch = Batch::new(BatchType::Unlogged);
            for _ in chunk {
                batch.append_statement(self.ps_data_point_insert.clone());
            }
            self.session
                .batch(&batch, chunk.to_vec())
                .await
                .map_err(|e| store_err("data_points batch insert", e))?;
        }

        self.index_string(ROW_KEY_METRIC_NAMES, &set.name).await?;
        for (tag_name, tag_value) in &set.tags {
            self.index_string(ROW_KEY_TAG_NAMES, tag_name).await?;
            self.index_string(ROW_KEY_TAG_VALUES, tag_value).await?;
        }
        Ok(())
    }

    async fn query(&self, query: &DatastoreQuery) -> Result<Vec<SeriesData>> {
        // Legacy (pre-1.1) value encoding is not supported yet.
        let row_keys: Vec<DataPointsRowKey> = self
            .matching_row_keys(query)
            .await?
            .into_iter()
            .filter(|rk| rk.data_type != DST_LEGACY)
            .collect();

        // Fan the per-row reads out concurrently, like the Java
        // semaphore-bounded async query path.
        let rows: Vec<(Tags, Vec<DataPoint>)> =
            stream::iter(row_keys.into_iter().map(|row_key| async move {
                let points = self.read_row(&row_key, query).await?;
                Ok::<_, Error>((row_key.tags, points))
            }))
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await?;

        let mut by_tags: BTreeMap<Tags, Vec<DataPoint>> = BTreeMap::new();
        for (tags, points) in rows {
            by_tags.entry(tags).or_default().extend(points);
        }

        let mut results = Vec::new();
        for (tags, mut points) in by_tags {
            points.sort_by_key(|p| p.timestamp_ms);
            if let Some(limit) = query.limit {
                points.truncate(limit);
            }
            if !points.is_empty() {
                results.push(SeriesData { tags, points });
            }
        }
        Ok(results)
    }

    async fn delete(&self, query: &DatastoreQuery) -> Result<()> {
        for row_key in self.matching_row_keys(query).await? {
            let start_col = if query.start_time_ms < row_key.row_time_ms {
                0
            } else {
                self.spec.column_name(row_key.row_time_ms, query.start_time_ms)
            };
            let end_col = self.spec.column_name(
                row_key.row_time_ms,
                query.end_time_ms.min(row_key.row_time_ms + self.spec.row_width_ms()),
            );
            self.session
                .execute_unpaged(
                    &self.ps_data_points_delete_range,
                    (
                        row_key.to_bytes(),
                        start_col.to_be_bytes().to_vec(),
                        end_col.to_be_bytes().to_vec(),
                    ),
                )
                .await
                .map_err(|e| store_err("data_points delete", e))?;
        }
        Ok(())
    }

    async fn metric_names(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let mut names = self.query_string_index(ROW_KEY_METRIC_NAMES).await?;
        if let Some(prefix) = prefix {
            names.retain(|n| n.starts_with(prefix));
        }
        Ok(names)
    }

    async fn tag_names(&self) -> Result<Vec<String>> {
        self.query_string_index(ROW_KEY_TAG_NAMES).await
    }

    async fn tag_values(&self) -> Result<Vec<String>> {
        self.query_string_index(ROW_KEY_TAG_VALUES).await
    }
}
