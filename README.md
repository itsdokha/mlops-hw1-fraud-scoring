# Скоринг транзакций из Kafka

Сервис читает транзакции из топика `transactions`, рассчитывает вероятность фрода моделью CatBoost на CPU и записывает `transaction_id`, `score`, `fraud_flag` в топик `scores`. Отдельный сервис сохраняет результаты в PostgreSQL. В Streamlit можно отправить CSV и посмотреть последние результаты.

## Что понадобится

- Docker Engine и Docker Compose V2 (команда `docker compose`);
- свободные порты 8501, 9095 и 5432;
- доступ к интернету при первой сборке образов.

Файлы модели уже лежат в `model/`. Датасеты для запуска не нужны.

## Запуск

```bash
git clone https://github.com/itsdokha/mlops-hw1-fraud-scoring.git
cd mlops-hw1-fraud-scoring
docker compose up --build -d
docker compose ps
```

Дождаться запуска сервисов `kafka`, `postgres`, `scorer`, `scores-sink` и `ui`. В выводе `docker compose ps` они должны иметь состояние `running`, а Kafka и PostgreSQL — `healthy`. Первый запуск может занять несколько минут из-за загрузки образов и Python-пакетов.

Интерфейс: <http://localhost:8501>.

## Проверка на примере

1. Открыть вкладку **Загрузить транзакции**.
2. Загрузить файл `sample_transactions.csv` из корня репозитория и нажать **Отправить в Kafka**.
3. Дождаться обработки сообщений, перейти во вкладку **Результаты** и нажать **Посмотреть результаты**. Если записей пока нет, повторить запрос через несколько секунд.
4. Проверить таблицу и гистограмму. Транзакция `sample-002` должна появиться в таблице фрода со скором около `0.6647`. Транзакция `sample-001` получает флаг `0` и отображается только на гистограмме.

Для проверки обеих записей напрямую в PostgreSQL:

```bash
docker compose exec postgres psql -U fraud -d fraud -c "SELECT transaction_id, score, fraud_flag FROM transaction_scores WHERE transaction_id LIKE 'sample-%' ORDER BY transaction_id;"
```

Для проверки сообщений выходного топика:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic scores --from-beginning --max-messages 2
```

Ожидаемый формат одного сообщения:

```json
{"transaction_id":"sample-002","score":0.6647,"fraud_flag":1}
```

Значение `score` в примере округлено. В Kafka и PostgreSQL хранится полное значение.

## Загрузка данных соревнования

Скачать `test.csv` по [ссылке на соревнование](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394) из задания. Во вкладке **Загрузить транзакции** выбрать этот файл и нажать **Отправить в Kafka**. Каждая строка CSV отправляется отдельным сообщением. Обработка большого файла может занять несколько минут. Для проверки прогресса открыть раздел **Результаты** или посмотреть логи.

CSV должен содержать как минимум колонки `transaction_time`, `amount`, `lat`, `lon`, `merchant_lat`, `merchant_lon`, `cat_id`, `population_city`. Дополнительные признаки модели: `merch`, `gender`, `us_state`, `jobs`. Если в файле нет `transaction_id`, интерфейс создаст его для каждой строки.

`sample_submition.csv` содержит только `index,prediction`: это шаблон ответа Kaggle, а не входные транзакции. Интерфейс отклонит такой файл.

## Логи и остановка

```bash
docker compose logs -f scorer scores-sink
docker compose down
```

`docker compose down` останавливает контейнеры и сохраняет данные PostgreSQL. Для проверки с пустой базой использовать `docker compose down -v`; эта команда удаляет сохранённые результаты.

## Модель и повторное обучение

Сервис использует готовый файл `model/fraud_catboost.cbm`; обучение внутри работающих контейнеров не выполняется. Порог фрода и метрики отложенной выборки находятся в `model/metrics.json`. Порог можно изменить переменной `FRAUD_THRESHOLD` у сервиса `scorer` в `docker-compose.yml`.

Для повторного обучения положить размеченный `train.csv` в папку `data/` и выполнить:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-train.txt
.venv/bin/python scripts/train_model.py
docker compose up --build -d
```

Папка `data/` исключена из Git и Docker-образа. Новые артефакты появятся в `model/`.

## Файлы проекта

| Путь | Назначение |
| --- | --- |
| `fraud_service/preprocessing.py` | Проверка колонок и подготовка признаков |
| `fraud_service/model.py` | CPU-инференс модели |
| `fraud_service/scorer_service.py` | Чтение `transactions` и запись `scores` |
| `fraud_service/sink_service.py` | Запись скоров из Kafka в PostgreSQL |
| `ui/app.py` | Загрузка CSV и просмотр результатов |
| `db/init.sql` | Создание таблицы результатов |
| `scripts/train_model.py` | Отдельный скрипт обучения |
| `docker-compose.yml` | Запуск всех сервисов |
