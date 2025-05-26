import logging
import json
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv
import os
import time
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection

# Load environment variables from .env file
load_dotenv()

TOPIC_NAME = "clickstream"

# PostgreSQL connection info loaded from environment variables
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = 16936
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PWD")

# OpenSearch connection info
OS_HOST = os.getenv("OPENSEARCH_HOST")  # e.g. 'https://your-opensearch-host:9200'
OS_USER = os.getenv("OPENSEARCH_USER")
OS_PASS = os.getenv("OPENSEARCH_PASS")
OS_INDEX = "clickstream_events"  # target index name in OpenSearch

# Session flush parameters
SESSION_TIMEOUT_SECONDS = 300  # 5 minutes inactivity to consider session expired
FLUSH_INTERVAL_SECONDS = 60    # Flush aggregated sessions every 60 seconds

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def create_tables_if_not_exists(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clickstream_events (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        session_id INT NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        event_timestamp TIMESTAMP NOT NULL,
        page_url VARCHAR(255) NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_metrics (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        session_id INT NOT NULL,
        page_view_count INT DEFAULT 0,
        click_count INT DEFAULT 0,
        scroll_count INT DEFAULT 0,
        session_start TIMESTAMP NOT NULL,
        session_end TIMESTAMP NOT NULL,
        UNIQUE(session_id)
    );
    """)

def insert_event_to_db(cur, event):
    insert_query = """
    INSERT INTO clickstream_events (user_id, session_id, event_type, event_timestamp, page_url)
    VALUES (%s, %s, %s, to_timestamp(%s), %s)
    """
    cur.execute(insert_query, (
        event['user_id'],
        event['session_id'],
        event['event_type'],
        event['event_timestamp'],
        event['page_url']
    ))

def upsert_session_metrics(cur, session):
    upsert_query = """
    INSERT INTO session_metrics (
        user_id, session_id, page_view_count, click_count, scroll_count, session_start, session_end
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (session_id) DO UPDATE SET
        page_view_count = session_metrics.page_view_count + EXCLUDED.page_view_count,
        click_count = session_metrics.click_count + EXCLUDED.click_count,
        scroll_count = session_metrics.scroll_count + EXCLUDED.scroll_count,
        session_start = LEAST(session_metrics.session_start, EXCLUDED.session_start),
        session_end = GREATEST(session_metrics.session_end, EXCLUDED.session_end)
    """
    cur.execute(upsert_query, (
        session['user_id'],
        session['session_id'],
        session['page_view_count'],
        session['click_count'],
        session['scroll_count'],
        session['session_start'],
        session['session_end'],
    ))

def flush_expired_sessions(sessions, cur):
    now = datetime.utcnow()
    expired_sessions = []
    for session_id, data in list(sessions.items()):
        last_event_time = data['session_end']
        if (now - last_event_time).total_seconds() > SESSION_TIMEOUT_SECONDS:
            try:
                upsert_session_metrics(cur, data)
                cur.connection.commit()
                logging.info(f"Flushed aggregated metrics for expired session {session_id} to DB.")
                expired_sessions.append(session_id)
            except Exception as e:
                logging.error(f"Failed to flush session {session_id} metrics: {e}")
                cur.connection.rollback()

    for session_id in expired_sessions:
        del sessions[session_id]

def main():
    sessions = {}  # session_id -> aggregate metrics dict
    last_flush_time = time.time()

    # Initialize OpenSearch client
    os_client = OpenSearch(
        hosts=[OS_HOST],
        http_auth=(OS_USER, OS_PASS),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            sslmode='require'
        )
        conn.autocommit = False
        cur = conn.cursor()

        create_tables_if_not_exists(cur)
        conn.commit()
        logging.info("Ensured clickstream_events and session_metrics tables exist.")

        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers="sa-test-assignment-kafka-sa-test-assignment.g.aivencloud.com:16938",
            security_protocol="SSL",
            ssl_cafile="../certs/ca.pem",
            ssl_certfile="../certs/service.cert",
            ssl_keyfile="../certs/service.key",
            auto_offset_reset='earliest',
            group_id="my-python-consumer-group",
            consumer_timeout_ms=10000
        )
        logging.info(f"Connected to Kafka, listening on topic '{TOPIC_NAME}'...")

        for message in consumer:
            msg_value = message.value.decode('utf-8')
            logging.info(f"Received message: {msg_value}")

            try:
                event = json.loads(msg_value)

                # Insert into PostgreSQL
                insert_event_to_db(cur, event)
                conn.commit()
                logging.info("Inserted event into PostgreSQL successfully.")

                # Index into OpenSearch
                # Transform event_timestamp from epoch to ISO8601 for OpenSearch
                event_doc = event.copy()
                event_doc['event_timestamp'] = datetime.utcfromtimestamp(event['event_timestamp']).isoformat()
                os_response = os_client.index(index=OS_INDEX, body=event_doc)
                logging.info(f"Indexed event into OpenSearch, ID: {os_response['_id']}")

            except Exception as e:
                logging.error(f"Failed to process event: {e}")
                conn.rollback()
                continue

            # Aggregate session metrics in memory
            session_id = event['session_id']
            event_time = datetime.utcfromtimestamp(event['event_timestamp'])

            if session_id not in sessions:
                sessions[session_id] = {
                    'user_id': event['user_id'],
                    'session_id': session_id,
                    'page_view_count': 0,
                    'click_count': 0,
                    'scroll_count': 0,
                    'session_start': event_time,
                    'session_end': event_time,
                }

            sess = sessions[session_id]
            sess['session_end'] = max(sess['session_end'], event_time)
            sess['session_start'] = min(sess['session_start'], event_time)

            if event['event_type'] == 'page_view':
                sess['page_view_count'] += 1
            elif event['event_type'] == 'click':
                sess['click_count'] += 1
            elif event['event_type'] == 'scroll':
                sess['scroll_count'] += 1

            # Periodically flush expired sessions
            current_time = time.time()
            if current_time - last_flush_time > FLUSH_INTERVAL_SECONDS:
                flush_expired_sessions(sessions, cur)
                last_flush_time = current_time

        logging.info("No new messages, closing consumer.")

        # Flush any remaining sessions on shutdown
        flush_expired_sessions(sessions, cur)

        cur.close()
        conn.close()

    except KafkaError as e:
        logging.error(f"Kafka error: {e}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
