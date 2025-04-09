import hashlib
import pandas as pd
import folium
import webbrowser
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import logging
from typing import Optional, Tuple, Dict, Any, List
import json
import os
from sqlalchemy import create_engine



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('flight_visualizer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

COORDS_CACHE_FILE = os.path.join(os.path.dirname(__file__), "airport_coords_cache.json")
MAP_OUTPUT_FILE = "flights_map.html"
DB_CONFIG = {
    "dbname": "air_data",
    "user": "postgres",
    "password": "rosatom",
    "host": "localhost",
    "port": "5432"
}
MAX_FLIGHTS = 1000


class AirportGeocoder:

    BASE_COORDS = {
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

    def __init__(self):
        self.geolocator = Nominatim(user_agent="rosatom_flight_visualizer")
        self.geocode = RateLimiter(
            self.geolocator.geocode,
            min_delay_seconds=1,
            max_retries=2,
            error_wait_seconds=5
        )
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Tuple[float, float]]:
        if os.path.exists(COORDS_CACHE_FILE):
            try:
                with open(COORDS_CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    return {**self.BASE_COORDS, **cache}
            except Exception as e:
                logger.warning(f"Could not load coordinates cache: {e}")
        return self.BASE_COORDS.copy()

    def _save_cache(self):
        try:
            new_coords = {
                k: v for k, v in self.cache.items()
                if k not in self.BASE_COORDS or v != self.BASE_COORDS.get(k)
            }
            with open(COORDS_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(new_coords, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save coordinates cache: {e}")

    def get_coordinates(self, iata_code: str) -> Optional[Tuple[float, float]]:
        if not iata_code or len(iata_code) != 3:
            return None

        iata_code = iata_code.upper()

        if iata_code in self.cache:
            return self.cache[iata_code]

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
                    logger.info(f"Found coordinates for {iata_code}: {coords}")
                    return coords
                time.sleep(1)
            except Exception as e:
                logger.warning(f"Geocoding error for {iata_code} ({query}): {e}")
                time.sleep(2)

        logger.warning(f"Could not find coordinates for airport: {iata_code}")
        return None


class FlightVisualizer:

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        self.geocoder = AirportGeocoder()

    def load_flights_data(self) -> pd.DataFrame:
        try:
            query = f"""
                SELECT 
                    flight_number, 
                    airline, 
                    origin, 
                    destination, 
                    aircraft_model,
                    scheduled_time,
                    scheduled_departure,
                    status,
                    icao_code
                FROM flights
                WHERE scheduled_time > NOW() - INTERVAL '7 days'
                ORDER BY scheduled_time DESC
                LIMIT {MAX_FLIGHTS}
            """
            df = pd.read_sql(query, self.engine)

            df['scheduled_time'] = pd.to_datetime(df['scheduled_time'])
            df['scheduled_departure'] = pd.to_datetime(df['scheduled_departure'])

            return df

        except Exception as e:
            logger.error(f"Database error: {e}")
            raise

    @staticmethod
    def _generate_color(airline: str) -> str:
        return f"#{hashlib.md5(airline.encode()).hexdigest()[:6]}"

    def _add_airport_markers(self, flight_map: folium.Map, flights_df: pd.DataFrame):
        airports = set(flights_df['origin']).union(set(flights_df['destination']))

        for airport in airports:
            coords = self.geocoder.get_coordinates(airport)
            if coords:
                folium.Marker(
                    location=coords,
                    popup=f"Аэропорт: {airport}",
                    icon=folium.Icon(color='blue', icon='plane', prefix='fa')
                ).add_to(flight_map)

    def _add_aircraft_legend(self, flight_map: folium.Map, flights_df: pd.DataFrame):
        grouped = flights_df.groupby(['airline', 'aircraft_model'])['flight_number'] \
            .apply(lambda x: sorted([f for f in x if pd.notna(f)])) \
            .reset_index()

        legend_html = """
        <div style="
            position: fixed; 
            bottom: 50px;
            left: 50px;
            width: 300px;
            max-height: 500px;
            overflow: auto;
            background-color: white;
            border: 2px solid grey;
            padding: 10px;
            font-size: 12px;
            z-index: 9999;
        ">
            <h4 style="margin-top:0; text-align: center;">Авиакомпании и модели самолетов</h4>
        """

        current_airline = None
        for _, row in grouped.iterrows():
            airline = row['airline']
            model = row['aircraft_model']
            flights = row['flight_number']

            if not flights:
                continue

            if airline != current_airline:
                color = self._generate_color(airline)
                legend_html += f"""
                <div style="
                    margin: 10px 0 5px 0; 
                    border-bottom: 1px solid #ccc; 
                    padding-bottom: 3px;
                ">
                    <i style="
                        background: {color};
                        width: 15px;
                        height: 15px;
                        display: inline-block;
                        margin-right: 5px;
                        vertical-align: middle;
                    "></i>
                    <b>{airline}</b>
                </div>
                """
                current_airline = airline

            legend_html += f"""
            <div style="margin-left: 20px; margin-bottom: 10px;">
                <div style="font-weight: bold; margin-bottom: 3px;">{model if pd.notna(model) else 'Не указано'}</div>
                <div style="
                    margin-left: 10px; 
                    font-size: 11px;
                    column-count: 2;
                    column-gap: 10px;
                ">
                    {', '.join(flights)}
                </div>
            </div>
            """

        legend_html += "</div>"
        flight_map.get_root().html.add_child(folium.Element(legend_html))

    def create_map(self) -> Tuple[folium.Map, int, List[str]]:
        flights_df = self.load_flights_data()

        if flights_df.empty:
            logger.warning("No flight data found")
            return folium.Map(), 0, []

        first_airport = flights_df.iloc[0]['origin']
        center = self.geocoder.get_coordinates(first_airport) or (55, 37)

        flight_map = folium.Map(
            location=center,
            zoom_start=5,
            tiles='CartoDB positron'
        )

        missing_airports = set()
        routes_added = 0

        for _, row in flights_df.iterrows():
            origin_coords = self.geocoder.get_coordinates(row['origin'])
            dest_coords = self.geocoder.get_coordinates(row['destination'])

            if not origin_coords:
                missing_airports.add(row['origin'])
            if not dest_coords:
                missing_airports.add(row['destination'])
            if not origin_coords or not dest_coords:
                continue

            popup_content = f"""
            <div style="width: 250px">
                <h4>{row['flight_number']} - {row['airline']}</h4>
                <p><b>From:</b> {row['origin']} ({row.get('icao_code', '')})</p>
                <p><b>To:</b> {row['destination']}</p>
                <p><b>Departure:</b> {row['scheduled_departure'].strftime('%Y-%m-%d %H:%M')}</p>
                <p><b>Arrival:</b> {row['scheduled_time'].strftime('%Y-%m-%d %H:%M')}</p>
                <p><b>Status:</b> {row['status']}</p>
                <p><b>Aircraft:</b> {row['aircraft_model']}</p>
            </div>
            """

            folium.PolyLine(
                locations=[origin_coords, dest_coords],
                popup=popup_content,
                tooltip=f"{row['flight_number']} ({row['airline']})",
                color=self._generate_color(row['airline']),
                weight=2,
                opacity=0.7
            ).add_to(flight_map)
            routes_added += 1

        self._add_airport_markers(flight_map, flights_df)
        self._add_aircraft_legend(flight_map, flights_df)

        return flight_map, routes_added, sorted(missing_airports)


def main_digest():
    try:
        logger.info("Starting flight data visualization...")

        visualizer = FlightVisualizer(DB_CONFIG)
        flight_map, routes_count, missing_airports = visualizer.create_map()

        flight_map.save(MAP_OUTPUT_FILE)
        logger.info(f"Flight map saved to {MAP_OUTPUT_FILE}")

        webbrowser.open(MAP_OUTPUT_FILE)

        logger.info(f"Successfully visualized {routes_count} flight routes")
        if missing_airports:
            logger.warning(f"Missing coordinates for airports: {', '.join(missing_airports)}")
            with open("missing_airports.txt", "w") as f:
                f.write("\n".join(missing_airports))

        logger.info("Visualization completed successfully")

    except Exception as e:
        logger.error(f"Visualization failed: {str(e)}", exc_info=True)
        raise


