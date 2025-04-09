import hashlib
import psycopg2
import pandas as pd
import folium
import webbrowser
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import logging
from typing import Optional, Tuple, Dict
import json
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


COORDS_CACHE_FILE = "airport_coords_cache.json"


BASE_AIRPORT_COORDS = {
    "PEE": (56.2503, 48.0538),
    "PKV": (58.4313, 31.3155),
    "IWA": (56.8522, 60.6544),
    "KVX": (57.1902, 40.5847),
    "MSQ": (53.88, 27.55),
    "MCX": (46.2208, 48.0384),
    "LED": (59.806084, 30.3083),
    "AER": (43.4489, 39.9569),
    "SVO": (55.9726, 37.4146),
    "DME": (55.4146, 37.8994),
    "VKO": (55.6033, 37.2922),
    "IST": (41.2611, 28.7422),
    "KUF": (53.5066, 50.1644),
    "TLV": (32.0114, 34.8867),
    "VAR": (43.2321, 27.8251),
    "DUS": (51.2809, 6.7573),
    "KUT": (42.178349, 42.491012),
    "AAQ": (44.8953, 37.3194),
    "BOJ": (42.5667, 27.5),
    "BUS": (41.6103, 41.6056),
    "MAN": (53.3650, -2.2725),
    "ALA": (43.3506, 77.0275),
}


class AirportGeocoder:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="airport_mapper")
        self.geocode = RateLimiter(
            self.geolocator.geocode,
            min_delay_seconds=2,
            max_retries=3,
            error_wait_seconds=5
        )
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Tuple[float, float]]:
        if os.path.exists(COORDS_CACHE_FILE):
            try:
                with open(COORDS_CACHE_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Не удалось загрузить кэш: {e}")
        return BASE_AIRPORT_COORDS.copy()

    def _save_cache(self):
        try:
            with open(COORDS_CACHE_FILE, 'w') as f:
                json.dump(self.cache, f)
        except Exception as e:
            logger.warning(f"Не удалось сохранить кэш: {e}")

    def get_coordinates(self, iata_code: str) -> Optional[Tuple[float, float]]:
        if iata_code in self.cache:
            return self.cache[iata_code]

        try:
            queries = [
                f"{iata_code} airport",
                f"{iata_code} international airport",
                f"{iata_code} airfield",
                iata_code
            ]

            for query in queries:
                try:
                    location = self.geocode(query, exactly_one=True)
                    if location:
                        coords = (location.latitude, location.longitude)
                        self.cache[iata_code] = coords
                        self._save_cache()
                        logger.info(f"Найдены координаты для {iata_code}: {coords}")
                        return coords
                except Exception as e:
                    logger.warning(f"Ошибка при геокодинге {iata_code} ({query}): {e}")
                    time.sleep(1)

            logger.warning(f"Не удалось найти координаты для аэропорта {iata_code}")
            return None

        except Exception as e:
            logger.error(f"Критическая ошибка при геокодинге {iata_code}: {e}")
            return None


def get_db_connection():
    conn = psycopg2.connect(
        dbname="air_data",
        user="postgres",
        password="rosatom",
        host="localhost",
        port="5432"
    )
    return conn

def load_flights_data() -> pd.DataFrame:
    try:
        with get_db_connection() as conn:
            query = """
                SELECT flight_number, airline, origin, destination, aircraft_model
                FROM flights
            """
            return pd.read_sql(query, conn)
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из базы: {e}")
        raise


def string_to_color(s: str) -> str:
    """Генерирует HEX-цвет из строки"""
    hash_object = hashlib.md5(s.encode())
    hex_color = "#" + hash_object.hexdigest()[:6]
    return hex_color


def create_flights_map(flights_df: pd.DataFrame, geocoder: AirportGeocoder) -> folium.Map:
    m = folium.Map(location=[45, 37], zoom_start=5)
    missing_airports = set()
    routes_added = 0

    for _, row in flights_df.iterrows():
        origin_coords = geocoder.get_coordinates(row['origin'])
        dest_coords = geocoder.get_coordinates(row['destination'])

        if origin_coords and dest_coords:
            folium.PolyLine(
                [origin_coords, dest_coords],
                tooltip=f"{row['flight_number']} ({row['airline']})",
                color=string_to_color(row['airline']),
                weight=2,
                opacity=0.6
            ).add_to(m)
            routes_added += 1
        else:
            if not origin_coords:
                missing_airports.add(row['origin'])
            if not dest_coords:
                missing_airports.add(row['destination'])

    for iata_code, coords in geocoder.cache.items():
        folium.Marker(
            coords,
            popup=f"Аэропорт: {iata_code}",
            icon=folium.Icon(color='green')
        ).add_to(m)

    return m, routes_added, missing_airports


def main_digest():
    try:
        geocoder = AirportGeocoder()

        logger.info("Загрузка данных о рейсах...")
        flights_df = load_flights_data()
        logger.info(f"Успешно загружено {len(flights_df)} рейсов")

        logger.info("Создание дэшборда...")
        m, routes_added, missing_airports = create_flights_map(flights_df, geocoder)

        # Добавим сноску со сгруппированными моделями и рейсами
        model_dict = flights_df.groupby("aircraft_model")["flight_number"].apply(list).to_dict()

        legend_html = """
        <div style="
            position: fixed;
            top: 50px;
            right: 10px;
            width: 300px;
            max-height: 500px;
            overflow-y: auto;
            background-color: white;
            border: 1px solid grey;
            padding: 10px;
            z-index: 1000;
            font-size: 13px;
        ">
        <b>🛩 Модели самолётов и рейсы:</b><br>
        """

        for model, flights in model_dict.items():
            legend_html += f"<b>{model}</b><br>"
            for flight in flights:
                legend_html += f"&nbsp;&nbsp;• {flight}<br>"
            legend_html += "<br>"

        legend_html += "</div>"

        m.get_root().html.add_child(folium.Element(legend_html))

        logger.info(f"- Всего рейсов: {len(flights_df)}")
        logger.info(f"- Успешно добавлено маршрутов: {routes_added}")
        logger.info(f"- Рейсов с отсутствующими координатами: {len(flights_df) - routes_added}")

        if missing_airports:
            logger.warning(f"\n Отсутствуют координаты для аэропортов: {', '.join(sorted(missing_airports))}")
            with open("missing_airports.txt", "w") as f:
                f.write("\n".join(sorted(missing_airports)))

        filename = "all_flights_map.html"
        m.save(filename)
        logger.info(f"\n Дешборд сохранён: {filename}")

        webbrowser.open(filename)

    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


