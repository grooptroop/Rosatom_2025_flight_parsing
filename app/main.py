import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

from data_parser.parser import main_parser
from data_digest.digest import main_digest

def main():
    print("\nВыберите действие:")
    print("1. Запустить парсер рейсов")
    print("2. Сгенерировать HTML-карту и метрики")
    choice = input("Введите номер действия: ")

    if choice == "1":
        main_parser()
    elif choice == "2":
        main_digest()
    else:
        print("Неверный выбор")

if __name__ == "__main__":
    main()
