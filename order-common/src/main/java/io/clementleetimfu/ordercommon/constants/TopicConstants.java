package io.clementleetimfu.ordercommon.constants;

public final class TopicConstants {

    public static final int PARTITIONS = 3;

    public static final int REPLICAS = 3;

    public static final String ORDER_PLACED = "order-placed";

    public static final String ORDER_CONFIRMED = "order-confirmed";

    public static final String ORDER_FAILED = "order-failed";

    private TopicConstants() {
    }

}
