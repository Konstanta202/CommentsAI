# SM Analytics - Система мониторинга комментариев ВК

##  Архитектура системы

```
VK Parser → Kafka → AI Service → Gateway → WebSocket → User Service → User Interface
```

### Компоненты системы:

1. **VK Parser** - Парсер комментариев из ВКонтакте
2. **AI Service** - Анализ комментариев с помощью ИИ
3. **Gateway Service** - Центральный сервис для обработки и маршрутизации
4. **User Service** - Управление пользователями и их настройками
5. **Kafka** - Message broker для асинхронной обработки
6. **Redis** - Кэширование и сессии
7. **PostgreSQL** - База данных пользователей


##  Поток данных

### 1. Сбор комментариев (VK Parser)
```bash
cd VKParser
mvn compile exec:java -Dexec.mainClass="ParserCLI" \
  -Dexec.args="--token YOUR_VK_TOKEN --groups GROUP_ID --watch"
```

### 2. Анализ комментариев (AI Service)
AI Service получает комментарии из Kafka, анализирует их и отправляет результаты обратно в Kafka.

### 3. Обработка результатов (Gateway Service)
Gateway Service получает результаты анализа и:
- Отправляет WebSocket уведомления пользователям
- Обновляет статистику
- Логирует события

### 4. Пользовательский интерфейс
Откройте http://localhost:8081 в браузере для просмотра real-time уведомлений.

##  Конфигурация

### Переменные окружения:

#### VK Parser:
- `VK_TOKEN` - Токен доступа ВКонтакте
- `KAFKA_TOPIC` - Топик Kafka для комментариев
- `KAFKA_BOOTSTRAP_SERVERS` - Адреса Kafka серверов

#### Gateway Service:
- `SPRING_KAFKA_BOOTSTRAP_SERVERS` - Адреса Kafka
- `SPRING_REDIS_HOST` - Хост Redis
- `KAFKA_TOPICS_COMMENT_ANALYSIS` - Топик для результатов анализа

#### User Service:
- `SPRING_DATASOURCE_URL` - URL базы данных
- `JWT_SECRET` - Секретный ключ для JWT токенов
