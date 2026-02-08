# --- Stage 1: Build the Distribution ---
FROM gradle:8.5-jdk21 AS builder
WORKDIR /home/gradle/project
COPY . .
RUN ./gradlew assembleDist --no-daemon

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Copy the TAR from the builder stage
COPY --from=builder /home/gradle/project/build/distributions/*.tar /app/app.tar

# Extract the TAR and remove the archive to save space
RUN tar -xvf app.tar --strip-components=1 && rm app.tar

# Copy the config to the root
COPY src/main/resources/application.yml application.yml

# Create data directory
RUN mkdir -p /app/data

EXPOSE 8080

ENTRYPOINT ["bin/storage-system"]