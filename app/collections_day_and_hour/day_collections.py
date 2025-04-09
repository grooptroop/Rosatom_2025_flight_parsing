# reports.py
import psycopg2
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FlightReport:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    def _get_connection(self):
        try:
            return psycopg2.connect(**self.db_config, connect_timeout=5)
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return None

    def get_flight_summary(self, icao_codes: List[str], date_from: str, date_to: str) -> List[Tuple]:
        query = """
            SELECT
                date_trunc('day', scheduled_time) AS flight_day,
                COALESCE(NULLIF(TRIM(airline), ''), 'Unknown Airline') AS airline,
                COALESCE(NULLIF(TRIM(aircraft_model), ''), 'Unknown Model') AS aircraft_model,
                COUNT(*) AS total_flights  
            FROM flights
            WHERE (origin = ANY(%s) OR destination = ANY(%s))
              AND scheduled_time BETWEEN %s AND %s
            GROUP BY flight_day, airline, aircraft_model
            ORDER BY total_flights DESC, flight_day, airline, aircraft_model;
        """

        conn = self._get_connection()
        if not conn:
            return []

        try:
            with conn.cursor() as cur:
                cur.execute(query, (icao_codes, icao_codes, date_from, date_to))
                result = cur.fetchall()
                logger.info(f"Retrieved {len(result)} rows from report")
                return result
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
        finally:
            conn.close()

    def save_summary_to_db(self, summary: List[Tuple]):
        insert_query = """
            INSERT INTO daily_flight_summary (flight_day, airline, aircraft_model, total_flights)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """

        conn = self._get_connection()
        if not conn or not summary:
            return

        try:
            with conn.cursor() as cur:
                cur.executemany(insert_query, summary)
                conn.commit()
                logger.info(f"Inserted {len(summary)} rows into daily_flight_summary")
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            conn.rollback()
        finally:
            conn.close()
