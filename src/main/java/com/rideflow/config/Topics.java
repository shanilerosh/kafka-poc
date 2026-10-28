package com.rideflow.config;

/** Topic names in one place. Dots are fine, but never mix '.' and '_' (Kafka metrics collide on them). */
public final class Topics {
    private Topics() {}

    public static final String RIDE_REQUESTED = "ride.requested";
    public static final String RIDE_MATCHED = "ride.matched";
    public static final String RIDE_COMPLETED = "ride.completed";
    public static final String PAYMENT_COMPLETED = "payment.completed";
    public static final String DRIVER_LOCATION = "driver.location";
    public static final String DRIVER_STATUS = "driver.status";
    public static final String SURGE_PRICING = "surge.pricing";

    // Dead Letter Topics: where messages go when processing keeps failing.
    public static final String RIDE_REQUESTED_DLT = RIDE_REQUESTED + ".DLT";
    public static final String RIDE_COMPLETED_DLT = RIDE_COMPLETED + ".DLT";
    // ride.matched uses @RetryableTopic, which creates ride.matched-retry-* and ride.matched-dlt itself.
}
