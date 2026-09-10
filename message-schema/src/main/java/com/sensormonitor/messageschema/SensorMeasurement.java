package com.sensormonitor.messageschema;

import java.time.Instant;

public record SensorMeasurement(
        String warehouseId,
        String sensorId,
        SensorType sensorType,
        double value,
        Instant timestamp
) {}
