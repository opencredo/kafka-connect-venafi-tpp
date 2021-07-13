package com.opencredo.connect.venafi.tpp.log.api.client;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.opencredo.connect.venafi.tpp.log.Deserializer.DotNetDateDeserializer;
import com.opencredo.connect.venafi.tpp.log.api.TppPlatformAuthorization;
import com.opencredo.connect.venafi.tpp.log.model.Credentials;
import com.opencredo.connect.venafi.tpp.log.model.TppToken;
import feign.Feign;
import feign.FeignException;
import feign.Retryer;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import feign.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

public class TokenClient {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TokenClient.class);
    private String tokenValue;
    private ZonedDateTime tokenExpiry = ZonedDateTime.now();
    private Credentials credentials;

    public TokenClient(String username, String password) {
        credentials = new Credentials(username, password);
    }

    private static GsonDecoder customDecoder() {
        return new GsonDecoder(
                new GsonBuilder()
                        .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                        .registerTypeAdapter(ZonedDateTime.class, new DotNetDateDeserializer())
                        .create());
    }

    public String getToken(String baseUrl) {
        if (isTokenInvalid()) {
            try {
                TppToken token = Feign
                        .builder()
                        .encoder(new GsonEncoder())
                        .logger(new Slf4jLogger())
                        .decoder(customDecoder())
                        .retryer(Retryer.NEVER_RETRY)
                        .target(TppPlatformAuthorization.class, baseUrl)
                        .getToken(credentials);

                tokenValue = token.getAPIKey();
                tokenExpiry = token.getValidUntil();
            } catch (FeignException e) {
                log.error("Caught following exception, ignoring to ensure connector doesn't fail", e);
                tokenValue = "";
                tokenExpiry = ZonedDateTime.now();
                return "";
            }
        }
        return tokenValue;

    }

    private boolean isTokenInvalid() {
        return tokenValue == null || isTokenExpired();
    }

    private boolean isTokenExpired() {
        return tokenExpiry.isBefore(ZonedDateTime.now().minusSeconds(10L));
    }
}
