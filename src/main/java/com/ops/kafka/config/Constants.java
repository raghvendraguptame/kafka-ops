package com.ops.kafka.config;

public class Constants {

    public static final String BOOTSTRAP_SERVER = "broker1.tapto.in:9092,broker2.tapto.in:9092,broker3.tapto.in:9092";
    public static final String STREAMS_INPUT_TOPIC = "streams-input-topic";
    public static final String STREAMS_ORDER_LEFT_TOPIC = "kstream-user-order-left";
    public static final String STREAMS_PROFILE_RIGHT_TOPIC = "kstream-user-profile-right";
    public static final String STREAMS_INNER_JOIN_TOPIC = "kstreams-inner-join";
    public static final String STREAMS_LEFT_JOIN_TOPIC = "kstreams-left-join";
    public static final String STREAMS_OUTER_JOIN_TOPIC = "kstreams-outer-join";
    public static final String STREAMS_OUTPUT_TOPIC = "streams-output-topic";
    public static final String STREAMS_AGG_OUTPUT_TOPIC = "streams-agg-output-topic";
    public static final String STREAMS_COUNT_OUTPUT_TOPIC = "streams-count-output-topic";
    public static final String STREAMS_REDUCE_OUTPUT_TOPIC = "streams-reduce-output-topic";
    public static final String TOPIC = "test-topic";
    public static final String CONSUMER_GROUP_NAME = "test-consumer";
    public static final String CONSUMER_GROUP_INSTANCE_ID = "";
    public static final String KSTREAMS_APP_ID = "kstream-app";
}
