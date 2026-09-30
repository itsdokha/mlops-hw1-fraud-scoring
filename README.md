# Потоковый скоринг транзакций

Учебный сервис получает строки CSV через Streamlit, отправляет каждую транзакцию в Kafka, выполняет препроцессинг и CPU-инференс, публикует `transaction_id`, `score`, `fraud_flag` в топик `scores` и сохраняет результаты в PostgreSQL. В интерфейсе доступны последние 10 транзакций с флагом фрода и распределение скоров последних 100 транзакций.

## Быстрый запуск

Нужны Docker Engine и Docker Compose V2 (`docker compose`). Порты 8501, 9095 и 5432 должны быть свободны.

```bash
docker compose up --build -d
docker compose ps
```

Откройте <http://localhost:8501>, загрузите `sample_transactions.csv` и нажмите **Отправить в Kafka**. Затем откройте вкладку **Результаты** и нажмите **Посмотреть результаты**. Обработка асинхронная: если результатов ещё нет, нажмите кнопку повторно через несколько секунд. Можно загрузить `data/test.csv` из соревнования. CSV должен иметь заголовок; каждая строка отправляется отдельным JSON-сообщением. Файл `data/sample_submition.csv` содержит только `index,prediction` и не является входным файлом транзакций; интерфейс отклонит его до отправки в Kafka.

Ожидаемый результат для демонстрационного файла: в таблице PostgreSQL появляются две записи; у `sample-001` флаг 0, у `sample-002` флаг 1.

Проверка без интерфейса:

```bash
docker compose exec postgres psql -U fraud -d fraud -c "SELECT transaction_id, score, fraud_flag FROM transaction_scores ORDER BY scored_at DESC LIMIT 10;"
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic scores --from-beginning --max-messages 2
docker compose logs scorer scores-sink
```

Остановка:

```bash
docker compose down
```

Для удаления данных PostgreSQL используйте `docker compose down -v`.

## Формат сообщений

Входной топик `transactions` содержит JSON:

```json
{"transaction_id":"sample-001","data":{"transaction_time":"2019-12-27 14:30","merch":"demo_store_1","cat_id":"1","amount":"25.50","lat":"40.75","lon":"-73.99","merchant_lat":"40.76","merchant_lon":"-73.98"}}
```

Поддерживается и плоский JSON с `transaction_id` и полями транзакции. UI берёт идентификатор из `transaction_id`, `trans_num` или `id`; если его нет, создаёт UUID. Выходной топик `scores` содержит ровно три поля:

```json
{"transaction_id":"sample-001","score":0.026,"fraud_flag":0}
```

Число `score` выше приведено для иллюстрации формата; фактическое значение рассчитывается моделью.

## Препроцессинг и модель

`fraud_service/preprocessing.py` строит признаки из суммы, времени, координат клиента и продавца, расстояния, населения города, категории покупки, пола, штата, профессии и продавца. Схема согласована с добавленными `data/train.csv` и `data/test.csv`: `transaction_time`, `merch`, `cat_id`, `amount`, `gender`, `us_state`, `lat`, `lon`, `population_city`, `jobs`, `merchant_lat`, `merchant_lon`. Файлы без обязательных колонок отклоняются; пустые значения и необязательные признаки заполняются нейтральными значениями.

`model/fraud_catboost.cbm` — CatBoost, обученный на предоставленном `data/train.csv`; `model/metrics.json` хранит порог и метрики отложенной выборки. Разбиение выполнено по времени транзакции: первые 80% для обучения, последние 20% для проверки. Сервис загружает готовый артефакт и выполняет только CPU-инференс. Порог берётся из `model/metrics.json`; при необходимости его можно переопределить переменной `FRAUD_THRESHOLD` у сервиса `scorer`.

На отложенной части получены ROC-AUC 0.9953, Average Precision 0.8335 и F1 0.7672 при пороге 0.1550. Порог выбран на этой же части данных, поэтому F1 следует считать оценкой подбора порога, а не результатом независимого теста.

Исходные данные исключены из Git и Docker-образа. Для повторного обучения вне сервиса:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-train.txt
.venv/bin/python scripts/train_model.py
docker compose up --build -d
```

## Состав проекта

- `fraud_service/scorer_service.py` — Kafka consumer/producer, вызов препроцессинга и модели;
- `fraud_service/preprocessing.py` — преобразование признаков;
- `fraud_service/model.py` — CPU-инференс;
- `scripts/train_model.py` — воспроизводимое офлайн-обучение;
- `fraud_service/sink_service.py` — Kafka consumer и идемпотентная запись в PostgreSQL;
- `db/init.sql` — создание витрины;
- `ui/app.py` — загрузка CSV и просмотр результатов;
- `docker-compose.yml` — Kafka, PostgreSQL и сервисы приложения.

Скоринг подтверждает Kafka offset после публикации результата, а сервис записи — после транзакции в PostgreSQL. При повторной доставке результат обновляется по `transaction_id`. Некорректные сообщения логируются и пропускаются.

## Локальная проверка кода

```bash
python3 -m unittest discover -s tests -v
```

Код и сервис предназначены для учебной локальной среды. Учётные данные PostgreSQL в Compose демонстрационные.
