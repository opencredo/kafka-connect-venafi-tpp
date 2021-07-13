package com.opencredo.connect.venafi.tpp.log;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.opencredo.connect.venafi.tpp.log.EventLogSourceTaskTest.*;
import static org.junit.jupiter.api.Assertions.*;

class TppLogSourceConnectorTest {

    public static final int ONE_MAX_TASK = 1;
    public static final int FIRST_VALUE_IN_LIST = 0;
    public static final int EXPECTED_NUMBER_OF_LOG_ENTRIES_RETURNED_BY_MOCK = 2;
    public static final int CALLED_ONCE = 1;
    public static final int TEN_MAX_TASKS = 10;
    private WireMockServer wireMockServer = new WireMockServer(
            new WireMockConfiguration().dynamicPort()
                    .extensions(new ResponseTemplateTransformer(false))
    );

    @BeforeEach
    private void setup() {
        wireMockServer.start();
    }

    @AfterEach
    private void shutdown() {
        wireMockServer.shutdown();
    }

    @Test
    public void as_a_connector_I_should_return_a_version() {
        TppLogSourceConnector source = given_a_source();
        assertEquals("test-version", source.version());
    }

    @Test
    void as_a_connector_I_should_be_able_to_start_up_with_some_properties() throws IllegalAccessException, InstantiationException, InterruptedException, NoSuchMethodException, InvocationTargetException {
        given_the_mock_will_respond_to_auth();
        given_the_mock_will_respond_to_log();

        TppLogSourceConnector source = given_a_source();
        when_the_source_is_started_with_minimum_properties(source);
        SourceTask sourceTask = then_I_should_be_able_to_get_a_source_task_from_the_connector(source);
        Map<String, String> taskProperties = then_I_can_get_the_task_properties(source);

        when_the_task_is_started(sourceTask, taskProperties);
        List<SourceRecord> records = then_the_task_can_be_polled(sourceTask);
        assertEquals(EXPECTED_NUMBER_OF_LOG_ENTRIES_RETURNED_BY_MOCK, records.size());
        wireMockServer.verify(CALLED_ONCE, postRequestedFor(urlPathMatching(AUTHORIZE_API_REGEX_PATH)));
        wireMockServer.verify(CALLED_ONCE, getRequestedFor(urlPathMatching(LOG_API_REGEX_PATH)));
    }

    @Test
    void as_a_connector_I_should_only_return_one_config_even_if_more_are_provided() throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        TppLogSourceConnector source = given_a_source();
        when_the_source_is_started_with_minimum_properties(source);
        SourceTask sourceTask = then_I_should_be_able_to_get_a_source_task_from_the_connector(source);
        List<Map<String, String>> taskProperties = then_I_can_get_the_task_properties(TEN_MAX_TASKS, source);
        assertEquals(taskProperties.size(), 1);
    }

    @Test
    void as_a_connector_I_should_throw_config_exception_if_provided_invalid_config() {
        TppLogSourceConnector source = given_a_source();
        Executable executingConfig = when_the_source_is_started_with_invalid_config(source);
        then_I_expect_a_(ConnectException.class, executingConfig);
    }

    @Test
    void as_a_connector_I_should_have_a_not_null_config_definition() {
        TppLogSourceConnector source = given_a_source();
        ConfigDef retrievedConfig = when_the_config_def_is_retrieved(source);
        assertNotNull(retrievedConfig);
    }

    private void then_I_expect_a_(Class<? extends Exception> clazz, Executable executingConfig) {
        assertThrows(clazz, executingConfig);
    }

    private Executable when_the_source_is_started_with_invalid_config(TppLogSourceConnector source) {
        return () -> when_the_source_is_started_with_properties(source, new HashMap<>());
    }

    private ConfigDef when_the_config_def_is_retrieved(TppLogSourceConnector source) {
        return source.config();
    }

    private List<SourceRecord> then_the_task_can_be_polled(SourceTask sourceTask) throws InterruptedException {
        return sourceTask.poll();
    }

    private void when_the_task_is_started(SourceTask sourceTask, Map<String, String> taskProperties) {
        sourceTask.start(taskProperties);
    }

    private Map<String, String> then_I_can_get_the_task_properties(TppLogSourceConnector connector) {
        return then_I_can_get_the_task_properties(ONE_MAX_TASK, connector).get(FIRST_VALUE_IN_LIST);
    }

    private List<Map<String, String>> then_I_can_get_the_task_properties(int maxTasks, TppLogSourceConnector connector) {
        return connector.taskConfigs(maxTasks);
    }

    private SourceTask then_I_should_be_able_to_get_a_source_task_from_the_connector(TppLogSourceConnector connector) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Constructor<? extends Task> noParamConstructor = connector.taskClass().getConstructor();
        final Task task = noParamConstructor.newInstance();
        assertTrue(task instanceof SourceTask);
        return (SourceTask) task;
    }

    private void when_the_source_is_started_with_minimum_properties(TppLogSourceConnector source) {
        Map<String, String> props = new HashMap<>();
        props.put(TppLogSourceConfig.BASE_URL_CONFIG, wireMockServer.baseUrl());
        props.put(TppLogSourceConfig.USERNAME_CONFIG, "placeholder_username");
        props.put(TppLogSourceConfig.PASSWORD_CONFIG, "placeholder_password");


        when_the_source_is_started_with_properties(source, props);
    }

    private void when_the_source_is_started_with_properties(TppLogSourceConnector connector, Map<String, String> props) {
        connector.start(props);
    }

    private TppLogSourceConnector given_a_source() {
        return new TppLogSourceConnector();
    }

    private void given_the_mock_will_respond_to_auth() {
        wireMockServer.stubFor(post(urlPathMatching(AUTHORIZE_API_REGEX_PATH))
                .withRequestBody(equalToJson("{\n" +
                        "\t\"Username\":\"placeholder_username\",\n" +
                        "\t\"Password\":\"placeholder_password\"\n" +
                        "}")).withHeader("Content-Type", containing("application/json"))
                .willReturn(okJson("{\n" +
                        "    \"APIKey\": \"{{randomValue type='UUID'}}\",\n" +
                        "    \"ValidUntil\": \"/Date(" + LocalDateTime.now().plusMinutes(3).toEpochSecond(ZoneOffset.UTC) + "000)/\"\n" +
                        "}").withTransformers("response-template")
                ));
    }

    private void given_the_mock_will_respond_to_log() {
        wireMockServer.stubFor(get(urlPathMatching(LOG_API_REGEX_PATH))
                .willReturn(okJson("{\n" +
                        "    \"LogEvents\": [\n" +
                        createLogEventBody(getTodayPlus(1)) + "," +
                        createLogEventBody(getTodayPlus(2)) +
                        "    ]\n" +
                        "}")
                ));
    }

}