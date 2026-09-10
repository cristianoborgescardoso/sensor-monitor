FROM maven:3.9.5-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml .
COPY message-schema ./message-schema
COPY warehouse ./warehouse
COPY center ./center

RUN mvn clean package -pl warehouse -am -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/warehouse/target/warehouse-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 3344/udp
EXPOSE 3355/udp

ENTRYPOINT ["java", "-jar", "app.jar"]
