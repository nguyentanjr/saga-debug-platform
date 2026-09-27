package com.example.correlation.domain;

/** Origin of an event's timestamp, independent of the transport carrying the event. */
public enum EventTimeSource {
    /** A usable event-time value supplied at the location declared by the source mapping. */
    DECLARED,
    /** No usable declared event time; the transport's timestamp was used as a fallback. */
    TRANSPORT
}
