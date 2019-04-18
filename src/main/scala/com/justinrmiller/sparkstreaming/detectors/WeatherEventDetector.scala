package com.justinrmiller.sparkstreaming.detectors

import com.justinrmiller.sparkstreaming.{DetectedEvent, WeatherEvent, WeatherEventState}

abstract class WeatherEventDetector extends Serializable {
  def key: String

  def message: String

  def include(we: WeatherEvent): Boolean

  def emit(we: WeatherEventState): Option[DetectedEvent]

  def extractState(we: WeatherEvent): Option[WeatherEventState] = {
    if (this.include(we)) {
      Some(WeatherEventState(
        key = Option(this.key),
        earliest = Option(we.occurredAt),
        latest = Option(we.occurredAt),
        sensorId = Option(we.sensorId),
        customerId = Option(we.customerId),
        nearestAirport = Option(we.nearestAirport),
        tempF = Set(we.tempF)
      ))
    } else {
      None
    }
  }
}
