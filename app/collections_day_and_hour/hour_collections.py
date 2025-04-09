# hourly_reports.py
import psycopg2
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HourlyFlightReport:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    def _get_connection(self):
        try:
            return psycopg2.connect(**self.db_config, connect_timeout=5)
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return None

    def get_hourly_summary(self, icao_codes: List[str], hour: int) -> List[Tuple]:
        query = """
            SELECT
                date_trunc('hour', scheduled_time) AS flight_hour,
                COALESCE(NULLIF(TRIM(airline), ''), 'Unknown Airline') AS airline,
                COALESCE(NULLIF(TRIM(aircraft_model), ''), 'Unknown Model') AS aircraft_model,
                COUNT(*) AS total_flights
            FROM flights
            WHERE (origin = ANY(%s) OR destination = ANY(%s))
              AND EXTRACT(HOUR FROM scheduled_time) = %s
            GROUP BY flight_hour, airline, aircraft_model
            ORDER BY total_flights DESC, airline;
        """

        conn = self._get_connection()
        if not conn:
            return []

        try:
            with conn.cursor() as cur:
                logger.debug(f"Executing query: {cur.mogrify(query, (icao_codes, icao_codes, hour))}")
                cur.execute(query, (icao_codes, icao_codes, hour))
                result = cur.fetchall()
                logger.info(f"Retrieved {len(result)} records for hour {hour}")
                return result
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
        finally:
            conn.close()

    def save_hourly_summary(self, summary: List[Tuple]):
        if not summary:
            return

        insert_query = """
            INSERT INTO hourly_flight_summary 
                (flight_hour, airline, aircraft_model, total_flights)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (flight_hour, airline, aircraft_model) 
            DO UPDATE SET total_flights = EXCLUDED.total_flights;
        """

        conn = self._get_connection()
        if not conn:
            return

        try:
            with conn.cursor() as cur:
                cur.executemany(insert_query, summary)
                conn.commit()
                logger.info(f"Saved {len(summary)} hourly records")
        except Exception as e:
            logger.error(f"Failed to save hourly data: {e}")
            conn.rollback()
        finally:
            conn.close()