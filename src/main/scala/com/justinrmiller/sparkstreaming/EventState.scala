package com.justinrmiller.sparkstreaming

object WeatherEvent {
  def apply(fields: Map[String, String]): WeatherEvent = WeatherEvent(
    receivedAt = fields("receivedAt").toLong,
    occurredAt = fields("occurredAt").toLong,
    sensorId = fields("sensorId").toLong,
    customerId = fields("customerId").toLong,
    nearestAirport = fields("nearestAirport"),
    tempF = fields("tempF").toFloat
  )
}

case class WeatherEvent(
                         receivedAt: Long,
                         occurredAt: Long,
                         sensorId: Long,
                         customerId: Long,
                         nearestAirport: String,
                         tempF: Float
                       )

case class WeatherEventState(
                              key: Option[String] = None,
                              earliest: Option[Long] = None,
                              latest: Option[Long] = None,
                              sensorId: Option[Long] = None,
                              customerId: Option[Long] = None,
                              nearestAirport: Option[String],
                              tempF: Set[Float]
                            )

case class DetectedEvent(
                          occurredAt: Long,
                          observedAt: Long,
                          sensorId: Long,
                          customerId: Long,
                          nearestAirport: String,
                          tempF: Set[Float],
                          message: String
                        )

object WeatherEventStateMonoid {
  def zero: WeatherEventState = WeatherEventState(
    key = Option.empty,
    earliest = Option.empty,
    latest = Option.empty,
    sensorId = Option.empty,
    customerId = Option.empty,
    nearestAirport = Option.empty,
    tempF = Set.empty
  )

  def plus(a: WeatherEventState, b: WeatherEventState): WeatherEventState = {
    if (a == zero) { b }
    else if (b == zero) { a } else {
      WeatherEventState(
        key = a.key.orElse(b.key),
        earliest = if (a.earliest.get < b.earliest.get) a.earliest else b.earliest,
        latest = if (a.latest.get > b.latest.get) a.latest else b.latest,
        sensorId = a.sensorId.orElse(b.sensorId),
        customerId = a.customerId.orElse(b.customerId),
        nearestAirport = a.nearestAirport,
        tempF = a.tempF ++ b.tempF
      )
    }
  }
}

