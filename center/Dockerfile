# Build stage
FROM maven:3.9.5-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the parent pom and all modules
COPY pom.xml .
COPY message-schema ./message-schema
COPY warehouse ./warehouse
COPY center ./center

# Build only the center module and its dependencies
RUN mvn clean package -pl center -am -DskipTests

# Run stage
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/center/target/center-1.0.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
