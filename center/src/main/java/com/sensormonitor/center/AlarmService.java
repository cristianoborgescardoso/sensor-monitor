package com.sensormonitor.center;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.sensormonitor.messageschema.SensorMeasurement;
import com.sensormonitor.messageschema.SensorType;

@Log4j2
@Service
public class AlarmService {

    @Value("${thresholds.temperature:35.0}")
    private double tempThreshold;

    @Value("${thresholds.humidity:50.0}")
    private double humidityThreshold;

    public void processMeasurement(SensorMeasurement measurement) {
        log.info("Received from Warehouse: {}", measurement);

        boolean alarm = false;
        if (measurement.sensorType() == SensorType.TEMPERATURE && measurement.value() > tempThreshold) {
            alarm = true;
        } else if (measurement.sensorType() == SensorType.HUMIDITY && measurement.value() > humidityThreshold) {
            alarm = true;
        }

        if (alarm) {
            log.warn("ALARM! Warehouse {} - Sensor {} exceeded threshold. Value: {}", measurement.warehouseId(), measurement.sensorId(), measurement.value());
        }
    }
}
