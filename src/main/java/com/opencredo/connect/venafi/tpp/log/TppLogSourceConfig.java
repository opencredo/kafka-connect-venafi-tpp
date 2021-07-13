package com.opencredo.connect.venafi.tpp.log;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TppLogSourceConfig extends AbstractConfig {
    public static final String BASE_URL_CONFIG = "venafi.base.url";
    private static final String BASE_URL_DOC = "URL to VENAFI VEDSDK API";

    public static final String USERNAME_CONFIG = "venafi.username";
    private static final String USERNAME_DOC = "The username to use with the API.";

    public static final String PASSWORD_CONFIG = "venafi.password";
    private static final String PASSWORD_DOC = "The password to use with the API.";

    public static final String TOPIC_CONFIG = "venafi.topic";
    private static final String TOPIC_DEFAULT = "VENAFI-LOGS";
    private static final String TOPIC_DOC = "Topic to publish VENAFI log data to.";

    public static final String BATCH_SIZE = "venafi.batch.size";
    private static final int BATCH_SIZE_DEFAULT = 100;
    private static final String BATCH_SIZE_DOC = "Window of data to pull from log API.";

    public static final String POLL_INTERVAL = "venafi.poll.interval";
    private static final int POLL_INTERVAL_DEFAULT = 1000;
    private static final String POLL_INTERVAL_DOC = "Poll interval in milliseconds.";
    private static final Logger log = LoggerFactory.getLogger(TppLogSourceConfig.class);

    public static final int MAX_BATCH_SIZE = 10_000;
    public static final int MIN_BATCH_SIZE = 2;
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BASE_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyStringWithoutControlChars(), ConfigDef.Importance.HIGH, BASE_URL_DOC)
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, TOPIC_DEFAULT, new NonEmptyStringWithoutControlChars(), ConfigDef.Importance.HIGH, TOPIC_DOC)
            .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Range.between(MIN_BATCH_SIZE, MAX_BATCH_SIZE), ConfigDef.Importance.LOW, BATCH_SIZE_DOC)
            .define(POLL_INTERVAL, ConfigDef.Type.INT, POLL_INTERVAL_DEFAULT, ConfigDef.Importance.LOW, POLL_INTERVAL_DOC)
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, USERNAME_DOC)
            .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PASSWORD_DOC);

    public TppLogSourceConfig(Map<String, ?> props) {
        super(CONFIG_DEF, props);
    }

    Map<String, String> returnPropertiesWithDefaultsValuesIfMissing() {
        Map<String, ?> uncastProperties = this.values();
        Map<String, String> config = new HashMap<>(uncastProperties.size());
        uncastProperties.forEach((key, valueToBeCast) -> config.put(key, valueToBeCast.toString()));

        return config;
    }
}

final class NonEmptyStringWithoutControlChars extends ConfigDef.NonEmptyStringWithoutControlChars {
    //Only here to create nice human readable for exporting to documentation.
    @Override
    public String toString() {
        return "non-empty string and no ISO control characters";
    }
}

