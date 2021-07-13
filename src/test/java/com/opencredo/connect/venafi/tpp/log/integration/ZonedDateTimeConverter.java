package com.opencredo.connect.venafi.tpp.log.integration;

import com.google.gson.*;
import com.opencredo.connect.venafi.tpp.log.Deserializer.ZonedDateTimeDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Gson converter for ZonedDateTime
 */
class ZonedDateTimeConverter implements JsonSerializer<ZonedDateTime>, JsonDeserializer<ZonedDateTime> {
    private static final Logger log = LoggerFactory.getLogger(ZonedDateTimeDeserializer.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private ZonedDateTime getParsedDate(String dateTimeString) {
        ZonedDateTime zonedDateTime = null;
        try {
            Instant instant = Instant.ofEpochMilli(Long.parseLong(dateTimeString));
            zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC").normalized());
        } catch (DateTimeParseException e) {
            //swallow exception for now
            log.debug("Failed to parse to LocalDateTime format", e);
        }
        return zonedDateTime;
    }

    @Override
    public ZonedDateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        if (jsonElement.isJsonNull() || jsonElement.getAsString().isEmpty()) {
            throw new JsonParseException("Unable to deserialize [" + jsonElement + "] to a ZoneDateTime.");
        }
        String json = jsonElement.getAsString();
        return getParsedDate(json);
    }

    @Override
    public JsonElement serialize(ZonedDateTime src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(FORMATTER.format(src));
    }
}
