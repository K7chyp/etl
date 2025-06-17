import csv
import random
from datetime import datetime, timedelta

# Настройки
ROWS = 10 ** 5
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 6, 1)

# Категории транзакций
CATEGORIES = [
    "food", "electronics", "transport", 
    "shopping", "entertainment", "utilities",
    "health", "education", "travel"
]

# Генерация данных
data = []
for i in range(1, ROWS + 1):
    user_id = random.randint(1001, 1100)
    days_diff = random.randint(0, (END_DATE - START_DATE).days)
    transaction_date = (START_DATE + timedelta(days=days_diff)).strftime("%Y-%m-%d")
    amount = round(random.uniform(5.0, 500.0), 2)
    merchant_id = random.randint(5001, 5100)
    category = random.choice(CATEGORIES)
    
    data.append([
        i,  # transaction_id
        user_id,
        transaction_date,
        amount,
        merchant_id,
        category
    ])

# Сохранение в CSV
with open('transactions_v2.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "user_id", "transaction_date", "amount", "merchant_id", "category"])
    writer.writerows(data)

print(f"Сгенерировано {ROWS} записей в transactions_v2.csv")