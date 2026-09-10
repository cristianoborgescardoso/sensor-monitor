package com.sensormonitor.center;

import lombok.extern.log4j.Log4j2;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sensormonitor.messageschema.SensorMeasurement;
import com.sensormonitor.messageschema.MqttTopics;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;

@Log4j2
@Component
public class MqttSensorSubscriber implements MqttCallback {

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String brokerUrl;

    @Value("${mqtt.topic:" + MqttTopics.SENSOR_WILDCARD_TOPIC + "}")
    private String topic;

    @Value("${mqtt.client.prefix:CenterSubscriber-}")
    private String mqttClientPrefix;

    private final AlarmService alarmService;
    private final ObjectMapper objectMapper;
    private MqttClient mqttClient;
    private volatile boolean keepRunning = true;
    private MqttConnector connectorThread;

    @org.springframework.beans.factory.annotation.Autowired
    public MqttSensorSubscriber(AlarmService alarmService) {
        this.alarmService = alarmService;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @PostConstruct
    public void start() {
        connectorThread = new MqttConnector();
        connectorThread.start();
    }

    private class MqttConnector extends Thread {
        public MqttConnector() {
            super("MqttConnectorThread");
        }

        @Override
        public void run() 
        {
            while (keepRunning && mqttClient == null) 
            {
                try 
                {
                    initMqttClient();
                    log.info("MQTT Client connected. Url: {}", brokerUrl);
                } 
                catch (Exception e) 
                {
                    log.error("Failed to initialize MQTT Client. Url: {}. Retrying in 1s...", brokerUrl, e);
                    try 
                    {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void initMqttClient() throws MqttException 
    {
        String clientId = mqttClientPrefix + UUID.randomUUID().toString();
        mqttClient = new MqttClient(brokerUrl, clientId, null);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        mqttClient.setCallback(this);
        mqttClient.connect(options);
        mqttClient.subscribe(topic, 1);

        log.info("Connected to MQTT Broker at {} and subscribed to '{}'", brokerUrl, topic);
    }

    @PreDestroy
    public void stop() {
        keepRunning = false;
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close();
            }
        } catch (MqttException e) {
            log.error("Error closing MQTT client", e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("Lost connection to MQTT broker", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        String payload = new String(message.getPayload());
        log.info("Received MQTT message on '{}': {}", topic, payload);

        try {
            SensorMeasurement measurement = objectMapper.readValue(payload, SensorMeasurement.class);
            alarmService.processMeasurement(measurement);
        } catch (Exception e) {
            log.warn("Failed to parse or process MQTT message", e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Not used by subscriber
    }
}
