package com.opencredo.connect.venafi.tpp.log.api.client;

import com.google.gson.GsonBuilder;
import com.opencredo.connect.venafi.tpp.log.Deserializer.ZonedDateTimeDeserializer;
import com.opencredo.connect.venafi.tpp.log.api.TppLog;
import com.opencredo.connect.venafi.tpp.log.model.LogResponse;
import feign.Feign;
import feign.Retryer;
import feign.gson.GsonDecoder;
import feign.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;

public class LogsClient {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LogsClient.class);

    public static LogResponse getLogs(String token, String date, String baseUrl, String batchSize, long offset) {
        try {
            return Feign.builder()
                    .logger(new Slf4jLogger())
                    .decoder(logDecoder())
                    .retryer(Retryer.NEVER_RETRY)
                    .target(TppLog.class, baseUrl)
                    .getLogs(token, date, batchSize, offset);
        } catch (Exception e) {
            log.error("Caught following exception, ignoring to ensure connector doesn't fail", e);
            return new LogResponse(new ArrayList<>());
        }
    }

    private static GsonDecoder logDecoder() {
        return new GsonDecoder(
                new GsonBuilder()
                        .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeDeserializer())
                        .create());
    }
}
