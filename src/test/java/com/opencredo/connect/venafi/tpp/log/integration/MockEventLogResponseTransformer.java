package com.opencredo.connect.venafi.tpp.log.integration;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencredo.connect.venafi.tpp.log.model.EventLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.opencredo.connect.venafi.tpp.log.api.TppLog.LIMIT;

/**
 * Builds the response for the Mocked Venafi LogEvents service
 */
class MockEventLogResponseTransformer extends ResponseTemplateTransformer {

    private static List<EventLog> venafiLogs;
    private static List<List<EventLog>> partitions;

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeConverter())
            .create();

    private static final AtomicInteger requestNumber = new AtomicInteger(0);

    public MockEventLogResponseTransformer(boolean global) {
        super(global);
        venafiLogs = loadLogs();
    }

    @Override
    public ResponseDefinition transform(Request request, ResponseDefinition responseDefinition, FileSource fileSource, Parameters parameters) {
        QueryParameter queryParameter = request.queryParameter(LIMIT);

        List<EventLog> eventLogs = responseLogs(
                requestNumber.getAndIncrement(),
                Integer.parseInt(queryParameter.firstValue())
        );

        String logs = eventLogs.size() > 0 ? gson.toJson(eventLogs) : "[]";
        return new ResponseDefinitionBuilder()
                .withHeader("Content-Type", "application/json")
                .withStatus(200)
                .withBody("{\n" +
                        "    \"LogEvents\": \n" +
                                logs +
                        " \n" +
                        "}")
                .build();
    }

    @Override
    public String getName() {
        return "event-log-response-transformer";
    }

    private List<EventLog> loadLogs() {
        List<EventLog> eventLogs = new ArrayList<>();
        try (Stream<String> lines = Files.lines(Paths.get("src/test/resources/venafi-output-payload.json"))) {

            for (String line: lines.collect(Collectors.toList())) {
                EventLog eventLog = gson.fromJson(line, EventLog.class);
                eventLogs.add(eventLog);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while loading Venafi logs from file", e);
        }
        return eventLogs;
    }

    protected static List<EventLog> responseLogs(int requestNumber, int limit) {
        if (partitions == null) {
            partitions = Lists.partition(venafiLogs, limit);
        }
        return partitions.get(requestNumber);
    }
}