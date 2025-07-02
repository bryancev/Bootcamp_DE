"""
Сохраняет в csv файл почасовые значения
Температуры, Осадков, Давления за указанный диапазон дат.
Передаём параметры start/end, и координаты локаций.
"""

import requests
import csv
from datetime import datetime
import os

# Настройки API
API_URL = "https://api.open-meteo.com/v1/forecast"

# Список городов с координатами (широта, долгота)
CITIES = {
    "Москва": (55.7558, 37.6173),
    "Санкт-Петербург": (59.9343, 30.3351),
    "Екатеринбург": (56.8389, 60.6057),
    "Казань": (55.7963, 49.1084),
}

# Диапазон дат для прогноза
START_DATE = "2025-07-02"
END_DATE = "2025-07-03"

# Создаем папку tmp (если не существует)
os.makedirs("tmp", exist_ok=True)

def main():
    """Основная функция для получения и сохранения данных"""
    weather_data = []
    
    for city, (lat, lon) in CITIES.items():
        try:
            # Параметры запроса
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,rain",
                "start_date": START_DATE,
                "end_date": END_DATE,
                "timezone": "Europe/Moscow"
            }
            
            # Отправка запроса с таймаутом
            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Обработка данных с помощью enumerate
            for i, time in enumerate(data["hourly"]["time"]):
                weather_data.append({
                    "Date": time,
                    "City": city,
                    "Temperature": data["hourly"]["temperature_2m"][i],
                    "Rain": data["hourly"]["rain"][i],
                    "Update_at ": datetime.now().strftime("%Y-%m-%d %H:%M")
                })
                
        except requests.exceptions.RequestException as e:
            print(f"Ошибка для города {city}: {e}")
            continue
    
    # Сохраняем в CSV файл в папку tmp
    filename = f"tmp/weather_{START_DATE}_{END_DATE}.csv"
    with open(filename, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=weather_data[0].keys())
        writer.writeheader()
        writer.writerows(weather_data)
    
    # Выводим результат
    print(f"\nДанные сохранены в файл: {filename}")
    print(f"Всего записей: {len(weather_data)}")
    print("\nПервые 3 записи:")
    for row in weather_data[:3]:
        print(row)

if __name__ == "__main__":
    print(f"Сбор данных о погоде с {START_DATE} по {END_DATE}...")
    main()