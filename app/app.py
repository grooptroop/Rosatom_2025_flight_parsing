import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from app.data_parser.parser import main_parser
from app.data_digest.digest import main_digest

def main():
    print("\nВыберите действие:")
    print("1. Запустить парсер рейсов")
    print("2. Сгенерировать HTML-карту и метрики")
    choice = input("Введите номер действия: ")

    if choice == "1":
        date_from = input("Введите дату начала (YYYY-MM-DD): ")
        date_to = input("Введите дату окончания (YYYY-MM-DD): ")
        hour_str = input("Введите час (0–23): ")

        try:
            hour = int(hour_str)
            if not (0 <= hour <= 23):
                raise ValueError("Час должен быть в диапазоне от 0 до 23")
        except ValueError as ve:
            print(f"Ошибка: {ve}")
            return

        # передаём параметры в main_parser
        main_parser(date_from, date_to, hour)
    elif choice == "2":
        main_digest()
    else:
        print("Неверный выбор")

if __name__ == "__main__":
    main()
