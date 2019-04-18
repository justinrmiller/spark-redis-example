package com.justinrmiller.sparkstreaming.detectors

import com.justinrmiller.sparkstreaming.{DetectedEvent, WeatherEvent, WeatherEventState}

class OutlierDetector extends WeatherEventDetector {
  override def key = "outlier"

  override def message = "Detected the following out of bounds temperatures: "

  override def include(we: WeatherEvent): Boolean = we.tempF < -50 || we.tempF > 150

  override def emit(wes: WeatherEventState): Option[DetectedEvent] = {
    if (wes.tempF.size >= 3) {
      DetectedEvent(
        occurredAt = wes.earliest.get,
        observedAt = System.currentTimeMillis(),
        sensorId = wes.sensorId.get,
        customerId = wes.customerId.get,
        nearestAirport = wes.nearestAirport.get,
        tempF = wes.tempF,
        message = this.message + s" - (${wes.tempF.mkString(", ")})"
      )
    }
  }
}
