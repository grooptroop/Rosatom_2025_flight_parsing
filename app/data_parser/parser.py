import requests
from datetime import datetime
import time
from app.data_parser.database import FlightDatabase
from app.collections_day_and_hour.day_collections import FlightReport
import logging
from app.collections_day_and_hour.hour_collections import HourlyFlightReport
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlightParser:
    def __init__(self, db_handler: FlightDatabase):
        self.db = db_handler
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })

    def _safe_get(self, data: dict, keys: list, default=None):
        for key in keys:
            try:
                data = data[key]
            except (KeyError, TypeError):
                return default
        return data

    def parse_flight(self, flight: dict, airport: str):
        try:
            scheduled_arrival_ts = self._safe_get(flight, ['flight', 'time', 'scheduled', 'arrival'], 0)
            scheduled_departure_ts = self._safe_get(flight, ['flight', 'time', 'scheduled', 'departure'], 0)

            origin_iata = self._safe_get(flight, ['flight', 'airport', 'origin', 'code', 'iata'], 'XXX')
            origin_icao = self._safe_get(flight, ['flight', 'airport', 'origin', 'code', 'icao'], 'N/A')

            model_data = self._safe_get(flight, ['flight', 'aircraft', 'model'], {})
            aircraft_model = model_data.get('text') or model_data.get('code') or 'Unknown'

            return {
                'flight_number': self._safe_get(flight, ['flight', 'identification', 'number', 'default'], 'UNKNOWN'),
                'airline': self._safe_get(flight, ['flight', 'airline', 'name'], 'Unknown'),
                'origin': origin_iata,
                'destination': airport,
                'scheduled_time': datetime.fromtimestamp(scheduled_arrival_ts),
                'scheduled_departure': datetime.fromtimestamp(scheduled_departure_ts) if scheduled_departure_ts else None,
                'status': self._safe_get(flight, ['flight', 'status', 'text'], 'Unknown'),
                'aircraft_model': aircraft_model,
                'icao_code': origin_icao
            }

        except Exception as e:
            logger.error(f"Flight parsing error: {e}")
            return None

    def process_airport(self, airport: str) -> bool:
        try:
            logger.info(f"Processing {airport}...")

            url = "https://api.flightradar24.com/common/v1/airport.json"
            params = {
                "code": airport,
                "plugin[]": "schedule",
                "plugin-setting[schedule][mode]": "arrivals",
                "limit": 20
            }

            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            flights_data = self._safe_get(data, ['result', 'response', 'airport', 'pluginData', 'schedule', 'arrivals', 'data'], [])

            if not flights_data:
                logger.warning(f"No flights data for {airport}")
                return False

            flights = []
            for flight in flights_data:
                parsed = self.parse_flight(flight, airport)
                if parsed:
                    flights.append(parsed)

            if not flights:
                logger.warning(f"No valid flights found for {airport}")
                return False

            return self.db.save_flights(flights)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {airport}: {e}")
            return False
        # в идеале расписать обработку парсинга порядка 5 раз но мне не хватило на это выделенного времени, я не успел адекватно реализовать в своей голове структуру этого кейса
        except Exception as e:
            logger.error(f"Unexpected error for {airport}: {e}")
            return False

def main_parser():

    # защитить доступ к важной информацией переменной окружения (занести в докерфайл)

    db_config = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT")
    }


    db = FlightDatabase(db_config)
    parser = FlightParser(db)
    reporter = FlightReport(db_config)
    hourly_reporter = HourlyFlightReport(db_config)

    airports = ["AER", "GDZ", "AAQ", "SIP", "KHE", "NLV", "ODS", "CND", "VAR", "BOJ", "IST", "ONQ", "NOP", "SZF", "OGU", "TZX", "RZV", "BUS", "KUT"]
    while True:
        for airport in airports:
            parser.process_airport(airport)

        logger.info("Цикл закончен, 5 минут до повторного парсинга...")
        summary = reporter.get_flight_summary(icao_codes=airports, date_from="2025-04-07", date_to="2025-04-08")
        data = hourly_reporter.get_hourly_summary(icao_codes=airports, hour=14)

        reporter.save_summary_to_db(summary)
        hourly_reporter.save_hourly_summary(data)

        time.sleep(300)




