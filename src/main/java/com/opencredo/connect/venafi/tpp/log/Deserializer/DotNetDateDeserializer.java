package com.opencredo.connect.venafi.tpp.log.Deserializer;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DotNetDateDeserializer implements JsonDeserializer<ZonedDateTime> {

    public static final int SLASH_DATE_LENGTH = "/DATE(".length();
    public static final int CLOSE_BRACKET_SLASH_LENGTH = ")/".length();


    @Override
    public ZonedDateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        if (jsonElement.isJsonNull()) {
            return null;
        }
        String json = jsonElement.getAsString();
        String dateString = json.substring(SLASH_DATE_LENGTH, json.length() - CLOSE_BRACKET_SLASH_LENGTH);
        ZoneOffset offset = ZoneOffset.UTC;

        if (dateString.contains("+")) {
            String[] dateParts = dateString.split("\\+");
            dateString = dateParts[0];
            offset = ZoneOffset.of("+" + dateParts[1]);
        } else if (dateString.contains("-")) {
            String[] dateParts = dateString.split("\\-");
            dateString = dateParts[0];
            offset = ZoneOffset.of("+" + dateParts[1]);
        }
        Instant i = Instant.ofEpochMilli(Long.parseLong(dateString));

        return ZonedDateTime.ofInstant(i, offset);
    }
}