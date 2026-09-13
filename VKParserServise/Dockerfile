# Стадия сборки
FROM maven:3.8.4-openjdk-17 as builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests


FROM openjdk:17-jdk-slim


WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar

EXPOSE 8181
ENTRYPOINT ["java", "-jar", "/app/app.jar"]