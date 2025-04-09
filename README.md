

Структура таблиц в postgres:

![image](https://github.com/user-attachments/assets/0e1ab780-514c-4642-a3f1-d711a0aaf08a)

![image](https://github.com/user-attachments/assets/a4860738-fc1f-4c43-a0e1-471592a30d24)

![image](https://github.com/user-attachments/assets/0df21f30-ff5b-4ee8-9c42-a6eb1f61f7c6)



Примеры данных:


air_data=# SELECT * FROM flights;
  id  | flight_number |           airline           | origin | destination |   scheduled_time    |     status      |      aircraft_model      | icao_code |        last_update         | scheduled_departure
------+---------------+-----------------------------+--------+-------------+---------------------+-----------------+--------------------------+-----------+----------------------------+---------------------
 2612 | SU1118        | Aeroflot                    | SVO    | AER         | 2025-04-09 21:10:00 | Landed 21:03    | Airbus A320-251N         | UUEE      | 2025-04-09 21:09:02.971776 | 2025-04-09 17:20:00
 2613 | EO506         | Ikar                        | CEK    | AER         | 2025-04-09 21:25:00 | Delayed 21:48   | Boeing 737-9GP(ER)       | USCC      | 2025-04-09 21:09:02.971776 | 2025-04-09 17:40:00
 2614 | U6461         | Ural Airlines               | DME    | AER         | 2025-04-09 21:25:00 | Estimated 21:35 | Airbus A320-214          | UUDD      | 2025-04-09 21:09:02.971776 | 2025-04-09 17:40:00
 2615 | DP348         | Pobeda                      | KVX    | AER         | 2025-04-09 21:55:00 | Estimated 21:29 | Boeing 737-8AL           | USKK      | 2025-04-09 21:09:02.971776 | 2025-04-09 18:05:00

 


air_data=# SELECT *  FROM daily_flight_summary;
 id  | flight_day |                 airline                 |     aircraft_model      | total_flights |         created_at
-----+------------+-----------------------------------------+-------------------------+---------------+----------------------------
 353 | 2025-04-09 | Turkish Airlines                        | Airbus A321-231         |            17 | 2025-04-09 22:55:08.907225
 354 | 2025-04-09 | Turkish Airlines                        | Airbus A321-271NX       |            17 | 2025-04-09 22:55:08.907225
 355 | 2025-04-09 | Turkish Airlines                        | Boeing 737-8F2          |             7 | 2025-04-09 22:55:08.907225
 356 | 2025-04-09 | Turkish Airlines                        | Airbus A330-343         |             4 | 2025-04-09 22:55:08.907225



air_data=# SELECT * FROM flights LIMIT 10;
  id  | flight_number |           airline           | origin | destination |   scheduled_time    |     status      |      aircraft_model      | icao_code |        last_update         | scheduled_departure
------+---------------+-----------------------------+--------+-------------+---------------------+-----------------+--------------------------+-----------+----------------------------+---------------------
 2612 | SU1118        | Aeroflot                    | SVO    | AER         | 2025-04-09 21:10:00 | Landed 21:03    | Airbus A320-251N         | UUEE      | 2025-04-09 21:09:02.971776 | 2025-04-09 17:20:00
 2613 | EO506         | Ikar                        | CEK    | AER         | 2025-04-09 21:25:00 | Delayed 21:48   | Boeing 737-9GP(ER)       | USCC      | 2025-04-09 21:09:02.971776 | 2025-04-09 17:40:00
 2614 | U6461         | Ural Airlines               | DME    | AER         | 2025-04-09 21:25:00 | Estimated 21:35 | Airbus A320-214          | UUDD      | 2025-04-09 21:09:02.971776 | 2025-04-09 17:40:00


