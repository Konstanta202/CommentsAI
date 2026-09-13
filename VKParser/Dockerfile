FROM maven:3.9.5-eclipse-temurin-17 AS build

WORKDIR /app

# Копируем pom.xml и загружаем зависимости
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Копируем исходный код и собираем
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Копируем собранный JAR (учитываем разные имена)
COPY --from=build /app/target/*.jar app.jar

# Создаем директорию для данных
RUN mkdir -p /app/data

# Указываем точку входа с аргументами
ENTRYPOINT ["java", "-jar", "app.jar"]
CMD ["--groups", "ria channel78news", \
     "--out-dir", "/app/data", \
     "--watch", \
     "--poll-every", "300", \
     "--recent-posts-for-comments", "20", \
     "--parallel-comments", "3"]