package com.rideflow.consumer;

public class NoDriverAvailableException extends RuntimeException {
    public NoDriverAvailableException(String zone) {
        super("No driver available in zone " + zone);
    }
}
