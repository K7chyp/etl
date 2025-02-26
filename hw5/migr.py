from pymongo import MongoClient
import json
from tqdm import tqdm  # Для прогресс-бара (опционально)

# 1. Подключение к MongoDB
client = MongoClient(
    host='localhost',
    port=27017,
    username='root',
    password='example',
    authSource='admin'
)

# 2. Выбор/создание базы данных и коллекции
db = client['airbnb']
collection = db['listings']

# 3. Очистка коллекции (truncate)
def truncate_collection():
    try:
        # Удаляем все документы в коллекции
        result = collection.delete_many({})
        print(f"Коллекция очищена. Удалено документов: {result.deleted_count}")
    except Exception as e:
        print(f"Ошибка при очистке коллекции: {str(e)}")

# 4. Загрузка и обработка JSON-файла
def import_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            total_lines = sum(1 for _ in file)  # Подсчет общего количества строк
            file.seek(0)  # Возвращаем указатель файла в начало

            for line in tqdm(file, total=total_lines, desc="Импорт данных"):
                data = json.loads(line.strip())
                
                # Преобразование типов данных
                data = transform_data_types(data)
                
                # Добавление колонки с городом
                data = add_city_column(data)
                
                # Вставка данных в коллекцию
                collection.insert_one(data)
                
        print("Импорт завершен успешно!")
    except Exception as e:
        print(f"Ошибка при импорте данных: {str(e)}")

# 5. Преобразование типов данных
def transform_data_types(data):
    # Преобразование числовых полей
    if 'price' in data and '$numberDecimal' in data['price']:
        data['price'] = float(data['price']['$numberDecimal'])
    
    if 'accommodates' in data and '$numberInt' in data['accommodates']:
        data['accommodates'] = int(data['accommodates']['$numberInt'])
    
    if 'bedrooms' in data and '$numberInt' in data['bedrooms']:
        data['bedrooms'] = int(data['bedrooms']['$numberInt'])
    
    if 'beds' in data and '$numberInt' in data['beds']:
        data['beds'] = int(data['beds']['$numberInt'])
    
    if 'number_of_reviews' in data and '$numberInt' in data['number_of_reviews']:
        data['number_of_reviews'] = int(data['number_of_reviews']['$numberInt'])
    
    # Преобразование дат
    if 'last_scraped' in data and '$date' in data['last_scraped']:
        data['last_scraped'] = data['last_scraped']['$date']['$numberLong']
    
    if 'calendar_last_scraped' in data and '$date' in data['calendar_last_scraped']:
        data['calendar_last_scraped'] = data['calendar_last_scraped']['$date']['$numberLong']
    
    if 'first_review' in data and '$date' in data['first_review']:
        data['first_review'] = data['first_review']['$date']['$numberLong']
    
    if 'last_review' in data and '$date' in data['last_review']:
        data['last_review'] = data['last_review']['$date']['$numberLong']
    
    return data

# 6. Добавление колонки с городом
def add_city_column(data):
    # Извлекаем город из поля address.market
    if 'address' in data and 'market' in data['address']:
        data['city'] = data['address']['market']
    else:
        data['city'] = None  # Если город не указан
    
    return data

# 7. Запуск импорта
if __name__ == "__main__":
    try:
        # Очистка коллекции перед импортом
        truncate_collection()
        
        # Импорт данных
        import_data('listingsAndReviews.json')
    except Exception as e:
        print(f"Ошибка: {str(e)}")