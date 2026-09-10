package com.sensormonitor.warehouse;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sensormonitor.messageschema.SensorMeasurement;
import com.sensormonitor.messageschema.SensorType;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import com.sensormonitor.messageschema.MqttTopics;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.time.Instant;

@Log4j2
@Component
public class WarehouseService {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Value("${udp.buffer.size:1024}")
    private int bufferSize;

    @Value("${sensor.temperature.port:3344}")
    private int temperaturePort;

    @Value("${sensor.humidity.port:3355}")
    private int humidityPort;

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String mqttBrokerUrl;

    @Value("${mqtt.client.prefix:WarehousePublisher-}")
    private String mqttClientPrefix;

    @Value("${warehouse.id:default-warehouse}")
    private String warehouseId;

    private final BlockingQueue<SensorMeasurement> queue;

    public WarehouseService(@Value("${queue.capacity:100000}") int queueCapacity) {
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
    }

    private volatile boolean keepRunning = true;
    private UdpProducer temperatureProducer;
    private UdpProducer humidityProducer;
    private MeasurementConsumer consumer;

    @PostConstruct
    public void start() {
        temperatureProducer = new UdpProducer("UDP-Temperature", temperaturePort, SensorType.TEMPERATURE);
        humidityProducer = new UdpProducer("UDP-Humidity", humidityPort, SensorType.HUMIDITY);
        consumer = new MeasurementConsumer("Measurement-Consumer");

        temperatureProducer.start();
        humidityProducer.start();
        consumer.start();
    }

    @PreDestroy
    public void stop() {
        log.info("Shutting down UDP receivers...");
        keepRunning = false;

        if (temperatureProducer != null) {
            temperatureProducer.close();
        }
        if (humidityProducer != null) {
            humidityProducer.close();
        }
    }

    private SensorMeasurement parse(String raw, SensorType type) {
        try {
            String[] parts = raw.trim().split(";");
            String sensorId = parts[0].split("=")[1].trim();
            double value = Double.parseDouble(parts[1].split("=")[1].trim());

            return new SensorMeasurement(warehouseId, sensorId, type, value, Instant.now());
        } catch (Exception ex) {
            log.warn("Invalid message: '{}'", raw);
            return null;
        }
    }

    private void insertAsCircularBuffer(SensorMeasurement measurement) {
        while (!queue.offer(measurement)) {
            queue.poll();
        }
    }

    private class UdpProducer extends Thread {

        private final int port;
        private final SensorType sensorType;
        private DatagramSocket socket;

        UdpProducer(String name, int port, SensorType sensorType) {
            super(name);
            this.port = port;
            this.sensorType = sensorType;
        }

        void close() {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }

        @Override
        public void run() {
            try {
                socket = new DatagramSocket(new InetSocketAddress("0.0.0.0", port));
                log.info("{} listening on port {}", sensorType, port);
                byte[] buffer = new byte[bufferSize];

                while (keepRunning) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    String raw = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    log.info("Received [{}:{}]: {}", sensorType, port, raw);

                    SensorMeasurement measurement = parse(raw, sensorType);
                    if (measurement != null) {
                        insertAsCircularBuffer(measurement);
                    }
                }
            } catch (Exception ex) {
                if (keepRunning) {
                    log.error("Error on UDP port {}: {}", port, ex.getMessage());
                }
            } finally {
                close();
                log.info("{} stopped on port {}", sensorType, port);
            }
        }
    }

    private class MeasurementConsumer extends Thread {
        private MqttClient mqttClient;

        MeasurementConsumer(String name) {
            super(name);
            try {
                mqttClient = new MqttClient(mqttBrokerUrl,
                        mqttClientPrefix + java.util.UUID.randomUUID().toString(), null);
                MqttConnectOptions options = new MqttConnectOptions();
                options.setAutomaticReconnect(true);
                options.setCleanSession(true);
                mqttClient.connect(options);
            } catch (Exception e) {
                log.error("Failed to initialize MQTT Client", e);
            }
        }

        private void forwardMeasurement(SensorMeasurement measurement) {
            try {
                String json = objectMapper.writeValueAsString(measurement);
                log.info("Publishing MQTT message for {}: {} = {} at {}",
                        measurement.sensorId(), measurement.sensorType(), measurement.value(), measurement.timestamp());

                if (mqttClient != null && mqttClient.isConnected()) {
                    MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
                    message.setQos(1);
                    String topic = MqttTopics.getSensorTopic(measurement.sensorId());
                    mqttClient.publish(topic, message);
                } else {
                    log.warn("MQTT client not connected. Dropped: {}", measurement.sensorId());
                }
            } catch (Exception e) {
                log.warn("Failed to publish to MQTT: {}", e.getMessage());
            }
        }

        @Override
        public void run() {
            log.info("Consumer started");
            try {
                while (keepRunning) {
                    while (!queue.isEmpty()) {
                        if (mqttClient == null || !mqttClient.isConnected()) {
                            break;
                        }
                        SensorMeasurement measurement = queue.take();
                        log.info("Processed: {}", measurement);
                        forwardMeasurement(measurement);
                    }
                    Thread.sleep(100);
                }
                // Graceful shutdown
                while (!queue.isEmpty()) {
                    SensorMeasurement measurement = queue.take();
                    log.info("Processed (Shutdown): {}", measurement);
                    forwardMeasurement(measurement);
                }

                log.info("Consumer stopped");
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.error("Consumer interrupted", ex);
            }
        }
    }
}
