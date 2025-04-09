Инструкция по использованию:

откройте терминал и введите: venv\Scripts\activate

Перейдите в директорию где находятся файлы: cd d:\Rosatom_2025

Запустите главный файл: python -m app.app

----------------------------------------------------------------------------------------------------------------------------------

Создание таблиц:


CREATE TABLE flights (
    id SERIAL PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    airline VARCHAR(50) NOT NULL,
    origin CHAR(3) NOT NULL,
    destination CHAR(3) NOT NULL,
    scheduled_time TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    aircraft_model VARCHAR(50) NOT NULL,
    icao_code CHAR(4) NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_departure TIMESTAMP NOT NULL
);

----------------------------------------------------------------------------------------------------------------------------------


CREATE TABLE daily_flight_summary (
    id SERIAL PRIMARY KEY,
    flight_day DATE NOT NULL,
    airline VARCHAR(50) NOT NULL,
    aircraft_model VARCHAR(50) NOT NULL,
    total_flights INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


----------------------------------------------------------------------------------------------------------------------------------


CREATE TABLE hourly_flight_summary (
    id SERIAL PRIMARY KEY,
    flight_hour TIMESTAMP NOT NULL,
    airline VARCHAR(30) NOT NULL,
    aircraft_model VARCHAR(10) NOT NULL,
    total_flights INTEGER NOT NULL
);


----------------------------------------------------------------------------------------------------------------------------------

Структура таблиц в postgres:

![image](https://github.com/user-attachments/assets/0e1ab780-514c-4642-a3f1-d711a0aaf08a)
![image](https://github.com/user-attachments/assets/a4860738-fc1f-4c43-a0e1-471592a30d24)
![image](https://github.com/user-attachments/assets/0df21f30-ff5b-4ee8-9c42-a6eb1f61f7c6)


----------------------------------------------------------------------------------------------------------------------------------

Примеры данных:


air_data=# SELECT * FROM flights;


![image](https://github.com/user-attachments/assets/515e4c77-99eb-4b52-a72c-4e4f16287039)



 ----------------------------------------------------------------------------------------------------------------------------------


air_data=# SELECT *  FROM daily_flight_summary;


![image](https://github.com/user-attachments/assets/179d3ea9-3048-4c08-bb14-b442fc1f6329)



----------------------------------------------------------------------------------------------------------------------------------

air_data=# SELECT * FROM flights;


![image](https://github.com/user-attachments/assets/20a4c1f6-a961-4e69-90a4-625dad43c152)


----------------------------------------------------------------------------------------------------------------------------------


Итоговое исполнение:

![image](https://github.com/user-attachments/assets/5261760a-5d7e-4f2f-8e92-1dda69ebb97f)


