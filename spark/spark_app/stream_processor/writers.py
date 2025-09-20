# /opt/spark-apps/stream_processor/writers.py
import io
import json
import math
from datetime import datetime
import concurrent.futures

from pyspark.sql import DataFrame

from pyspark.sql.functions import col, broadcast
from psycopg2.extras import execute_values

from .config import (
    COPY_CHUNK,
    ES_BULK_CHUNK,
    ANOMALY_THRESHOLD,
    Z_THRESHOLD,
    ES_INDEX,
    KAFKA_ALERT_TOPIC,
    logger,
)
from .helpers import (
    get_psycopg2_conn,
    row_to_copy_buffer,
    send_chunk_to_dlq,
    get_es_client,
    get_alert_producer,
    helpers as es_helpers,
)
from .validation import validate_df, publish_invalid_to_dlq


# ==========================
# 1) Write raw events to Postgres (via JDBC) - drop event_time
# existing helpers for COPY, send_chunk_to_dlq, foreach_write_raw...
# Minimal copy of previously working implementations (kept concise)
# ==========================


def foreach_write_raw(batch_df: DataFrame, batch_id: int):
    # Bulk insert raw events via psycopg2 COPY FROM STDIN.
    # - batch_df: must include column event_time (TimestampType) and other fields.
    # - We drop event_time column when writing to user_events (table expects 'timestamp' column).

    if batch_df.rdd.isEmpty():
        logger.info(f"[raw] Batch {batch_id} empty -> skip")
        return
    # Drop event_time before storing to user_events (schema does not include it)
    df_to_write = batch_df.drop("event_time")
    selected = df_to_write.select(
        col("user_id"),
        col("product_id"),
        col("action"),
        col("timestamp"),
        col("device_id"),
        col("device_type"),
        col("location"),
        col("user_segment"),
        col("ip_address"),
    )
    cols = [
        "user_id",
        "product_id",
        "action",
        "timestamp",
        "device_id",
        "device_type",
        "location",
        "user_segment",
        "ip_address",
    ]

    def process_partition(iterator):
        conn = get_psycopg2_conn()
        cur = conn.cursor()
        chunk = []
        sent = 0
        try:
            for row in iterator:
                chunk.append(row)
                if len(chunk) >= COPY_CHUNK:
                    buf = row_to_copy_buffer(chunk, cols)
                    try:
                        cur.copy_from(
                            buf,
                            "user_events",
                            sep="\t",
                            null="\\N",
                            columns=tuple(cols),
                        )
                        conn.commit()
                        sent += len(chunk)
                    except Exception as e:
                        conn.rollback()
                        logger.warning("Failed COPY-ing chunk to user_events: %s", e)
                        send_chunk_to_dlq(chunk, str(batch_id), str(e))
                    chunk = []
            if chunk:
                buf = row_to_copy_buffer(chunk, cols)
                try:
                    cur.copy_from(
                        buf,
                        "user_events",
                        sep="\t",
                        null="\\N",
                        columns=tuple(cols),
                    )
                    conn.commit()
                    sent += len(chunk)
                except Exception as e:
                    conn.rollback()
                    logger.warning("Failed COPY-ing chunk to user_events: %s", e)
                    send_chunk_to_dlq(chunk, str(batch_id), str(e))
            return sent
        finally:
            cur.close()
            conn.close()

    total_sent = selected.rdd.mapPartitions(process_partition).sum()
    logger.info(f"[raw] Batch {batch_id} COPY-ed {total_sent} rows to user_events")
    es = get_es_client()
    if es:
        doc = {
            "type": "raw_batch",
            "batch_id": batch_id,
            "records": total_sent,
            "logged_at": datetime.utcnow().isoformat(),
        }
        try:
            es.index(index=ES_INDEX, document=doc)
        except TypeError:
            es.index(index=ES_INDEX, body=doc)
        except Exception as e:
            logger.warning("Failed indexing document to Elasticsearch: %s", e)


# ----------------------
# Validation + routing foreachBatch entrypoint
# ----------------------
def foreach_validate_and_route(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        logger.info("[validate] Batch {batch_id} empty -> skip")
        return
    valid_df, invalid_df = validate_df(batch_df)
    # Publish invalid rows to DLQ
    publish_invalid_to_dlq(invalid_df, batch_id)
    # Write valid rows to raw table
    if not valid_df.rdd.isEmpty():
        foreach_write_raw(valid_df, batch_id)
    else:
        logger.info("[validate] Batch {batch_id} has no valid rows after validation -> skip writing")


# ==========================
# WINDOWED AGGREGATION -> summary + anomaly detection
# window: 1 minute, slide 30 seconds (short window as requested)
# watermark: 2 minutes
# ==========================
def write_summary_and_anomaly(batch_df: DataFrame, batch_id: int, window_type: str, users_df: DataFrame):
    """
    foreachBatch handler for aggregated windowed results.
    Writes summary into user_activity_summary via UPSERT, anomalies into anomalous_events (INSERT ON CONFLICT DO NOTHING),
    logs to ES and triggers Telegram alerts for anomalies.
    """
    
    if batch_df.rdd.isEmpty():
        logger.info("[agg] Batch {batch_id} ({window_type} window) is empty -> skip")
        return

        # materialize to local DF with proper columns
    summary_df = batch_df.coalesce(2).select(
            col("window").getField("start").alias("window_start"),
            col("window").getField("end").alias("window_end"),
            col("user_id"),
            col("event_count"),
        )   
        # convert to pandas or collect for psycopg2 upserts.
    enriched_df = summary_df.join(broadcast(users_df), "user_id", "left").select(
            col("window_start"),
            col("window_end"),
            col("user_id"),
            col("event_count"),
            col("name").alias("user_name"),
            col("region"),
        )
    rows = enriched_df.collect()

    if not rows:
        logger.info(f"[agg] Batch {batch_id} ({window_type} window) has no summary rows -> skip")
        return

    # Prepare data tuples for upsert
    upsert_tuples = [(r["window_start"], r["window_end"], int(r["user_id"]), int(r["event_count"])) for r in rows]
    # Compute simple batch-level statistics for z-score detection
    counts = [t[3] for t in upsert_tuples]
    mean = sum(counts) / len(counts) if counts else 0.0
    var = sum((x - mean) ** 2 for x in counts) / len(counts) if counts else 0.0
    stddev = math.sqrt(var)
    z_threshold = Z_THRESHOLD
    logger.info(f"[agg] Batch {batch_id} ({window_type} window) mean={mean:.2f}, stddev={stddev:.2f}, z_threshold={z_threshold}")
    
    # Connect to Postgres and upsert
    conn = get_psycopg2_conn()
    cur = conn.cursor()
    try:
        # Upsert into user_activity_summary:
        # window_start TIMESTAMP, window_end TIMESTAMP, user_id, event_count
        upsert_sql = """
        INSERT INTO user_activity_summary (window_start, window_end, user_id, event_count)
        VALUES %s
        ON CONFLICT (user_id, window_start, window_end)
        DO UPDATE SET event_count = EXCLUDED.event_count;
        """
        execute_values(cur, upsert_sql, upsert_tuples, page_size=200)
        conn.commit()
        logger.info(f"[agg] Batch {batch_id} ({window_type} window) upserted {len(upsert_tuples)} summaries")
    except Exception as e:
        conn.rollback()
        logger.exception(f"[ERROR] Failed upserting summaries for batch {batch_id}: {e}")
    finally:
        cur.close()
        conn.close()

    # Detect anomalies: event_count > ANOMALY_THRESHOLD AND z-score > z_threshold
    anomalies = []

    for i, (ws, we, uid, cnt) in enumerate(upsert_tuples):
        z = (cnt - mean) / stddev if stddev > 0 else None
        if cnt > ANOMALY_THRESHOLD and (z is not None and z >= z_threshold):
            severity = "high" if z > 5 else "medium" if z > 3 else "low"
            if severity in ["high", "medium"]:
                anomalies.append(
                    (
                        uid,
                        ws.isoformat(),
                        we.isoformat(),
                        cnt,
                        float(z) if z is not None else 0.0,
                        severity,
                        rows[i]["user_name"],
                        rows[i]["region"],
                    )
                )

    # Persist anomalies (psycopg2) with ON CONFLICT DO NOTHING
    if anomalies:
        conn = get_psycopg2_conn()
        cur = conn.cursor()
        try:
            anomaly_db_tuples = [(uid, ws_iso, we_iso, cnt) for uid, ws_iso, we_iso, cnt, _, _, _, _ in anomalies]
            insert_anom_sql = """
            INSERT INTO anomalous_events (user_id, window_start, window_end, event_count)
            VALUES %s
            ON CONFLICT (user_id, window_start, window_end) DO NOTHING;
            """
            # convert iso strings back to timestamps via psycopg2 by passing them as strings
            execute_values(cur, insert_anom_sql, anomaly_db_tuples, page_size=200)
            conn.commit()
            logger.info(f"[anomaly] Batch {batch_id} wrote {len(anomalies)} anomalies")
        except Exception as e:
            logger.error(f"[ERROR] Failed writing anomalies for batch {batch_id}: {e}")
        finally:
            cur.close()
            conn.close()
            
        # ES bulk index anomalies
        es = get_es_client()
        if es:
            docs = [
                {
                    "_index": ES_INDEX,
                    "_source": {
                        "type": "anomaly",
                        "user_id": uid,
                        "event_count": cnt,
                        "window_start": ws_iso,
                        "window_end": we_iso,
                        "z_score": z,
                        "severity": severity,
                        "user_name": user_name,
                        "region": region,
                        "detected_at": datetime.utcnow().isoformat(),
                    }
                }
                for (
                    uid,
                    ws_iso,
                    we_iso,
                    cnt,
                    z,
                    severity,
                    user_name,
                    region
                ) in anomalies
            ]
            try:
                es_helpers.bulk(es, docs, chunk_size=ES_BULK_CHUNK, raise_on_error=False)
                logger.info(f"[ES] Bulk indexed {len(docs)} anomalies")
            except Exception as e:
                logger.error(f"[ERROR] ES bulk index anomalies failed for batch {batch_id}: {e}")

        # Send alerts to Kafka alert topic (batch send & flush once)
        prod = get_alert_producer() # reuse existing producer
        if prod and anomalies: 
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                for uid, ws_iso, we_iso, cnt, z, severity, user_name, region in anomalies:
                    alert_msg = {
                        "user_id": uid,
                        "user_name": user_name,
                        "region": region,
                        "window_start": ws_iso,
                        "window_end": we_iso,
                        "event_count": cnt,
                        "z_score": z,
                        "severity": severity,
                        "detected_at": datetime.utcnow().isoformat()
                    }
                    executor.submit(lambda msg=alert_msg: open("/tmp/alerts.log", "a").write(json.dumps(msg) + "\n"))
                    try:
                        prod.send(KAFKA_ALERT_TOPIC, alert_msg)
                    except Exception as e:
                        logger.warning(f"[Kafka] Failed sending alert for anomaly: {e}")
                prod.flush(timeout=5)
                logger.info(f"[Kafka] Sent {len(anomalies)} anomaly alerts to {KAFKA_ALERT_TOPIC}")
        

        # Log batch summary to ES
        es = get_es_client()
        if es:
            summary_doc = {
                "type": "agg_batch",
                "batch_id": batch_id,
                "window_type": window_type,
                "records": len(upsert_tuples),
                "anomalies": len(anomalies),
                "mean_event_count": mean,
                "stddev_event_count": stddev,
                "z_threshold": z_threshold,
                "logged_at": datetime.utcnow().isoformat(),
            }
            try:
                es.index(index=ES_INDEX, document=summary_doc)
            except TypeError:
                es.index(index=ES_INDEX, body=summary_doc)
            except Exception as e:
                logger.exception(f"[ERROR] Failed logging agg batch summary to ES: {e}")
    