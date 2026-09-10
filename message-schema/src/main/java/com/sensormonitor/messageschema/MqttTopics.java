package com.sensormonitor.messageschema;

public final class MqttTopics {
    private MqttTopics() {} // Prevent instantiation

    public static final String SENSOR_BASE_TOPIC = "sensors/";
    public static final String SENSOR_WILDCARD_TOPIC = "sensors/#";

    public static String getSensorTopic(String sensorId) {
        return SENSOR_BASE_TOPIC + sensorId;
    }
}
