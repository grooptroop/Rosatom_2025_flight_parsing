import psycopg2
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlightDatabase:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    def _get_connection(self):
        try:
            conn = psycopg2.connect(
                **self.db_config,
                connect_timeout=5
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return None

    def save_flights(self, flights: List[Dict]) -> bool:
        if not flights:
            logger.warning("No flights data to save")
            return False

        conn = self._get_connection()
        if not conn:
            return False

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM flights 
                    WHERE flight_number = %s 
                    AND scheduled_time = %s
                """, (
                    flights[0]['flight_number'],
                    flights[0]['scheduled_time']
                ))

                insert_sql = """
                    INSERT INTO flights (
                        flight_number, airline, origin, 
                        destination, scheduled_time, scheduled_departure,
                        status, aircraft_model, icao_code
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                records = [
                    (
                        f.get('flight_number'),
                        f.get('airline'),
                        f.get('origin'),
                        f.get('destination'),
                        f.get('scheduled_time'),
                        f.get('scheduled_departure'),
                        f.get('status'),
                        f.get('aircraft_model'),
                        f.get('icao_code')
                    )
                    for f in flights
                ]

                cur.executemany(insert_sql, records)
                conn.commit()
                logger.info(f"Saved {len(flights)} flights")
                return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            return False
        finally:
            conn.close()
