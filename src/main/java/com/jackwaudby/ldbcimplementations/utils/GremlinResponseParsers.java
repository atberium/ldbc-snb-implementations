package com.jackwaudby.ldbcimplementations.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Result;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * This class includes a number of methods for parsing Gremlin responses from the Gremlin Server.
 * They are written in a manner to allow composition.
 */
@UtilityClass
public class GremlinResponseParsers {
    public static int parseIntValue(@NonNull Object list) {
        return parseInt(parseObjectValue(list, Object::toString, "0"));
    }

    public static int parseIntValue(@NonNull Map<String, Object> map, @NonNull String key) {
        return parseIntValue(map.get(key));
    }

    public static float parseFloatValue(@NonNull Map<String, Object> map, @NonNull String key) {
        return parseFloat(parseObjectValue(map.get(key), Object::toString, "0"));
    }

    public static long parseLongValue(@NonNull Map<String, Object> map, @NonNull String key) {
        return parseLong(parseObjectValue(map.get(key), Object::toString, "0"));
    }

    public static boolean parseBooleanValue(@NonNull Map<String, Object> map, @NonNull String key) {
        return parseBoolean(parseObjectValue(map.get(key), Object::toString, "false"));
    }

    @Nullable
    public static String parseStringValue(@NonNull Map<String, Object> map, @NonNull String key) {
        return parseObjectValue(map.get(key), Objects::toString, null);
    }

    public static List<String> parsePropertyStringList(@NonNull Map<String, Object> map, @NonNull String key) {
        return parsePropertyStringList(map.get(key));
    }

    public static List<String> parsePropertyStringList(@Nullable Object list) {
        return parseList(list)
                .map(Object::toString)
                .map(StringUtils::trim)
                .filter(StringUtils::isNotBlank)
                .collect(toList());
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(@Nullable Object object) {
        return object == null ? emptyMap() : ((Map<String, Object>) object);
    }

    public static Map<String, Object> resultToMap(@Nullable Result result) {
        return result == null ? emptyMap() : toMap(result.get(Map.class));
    }

    @Nullable
    public static String getResultError(@NonNull List<Result> results) {
        if (results.size() == 1 && results.get(0).getObject() instanceof Map) {
            final Map<String, Object> stringObjectMap = resultToMap(results.get(0));
            return stringObjectMap.containsKey("error") ? stringObjectMap.get("error").toString() : null;
        }

        return null;
    }

    public static String parseCountryName(@NonNull Map<String, Object> object) {
        return parseStringValue(parseValueMap(object, "country"), "name");
    }

    public static String parseCompanyName(@NonNull Map<String, Object> object) {
        return parseStringValue(parseValueMap(object, "company"), "name");
    }

    public static String parseUniversityName(@NonNull Map<String, Object> object) {
        return parseStringValue(parseValueMap(object, "university"), "name");
    }

    public static int parseClassYear(@NonNull Map<String, Object> object) {
        return parseInt(parseValueMap(object, "studyFrom").get("classYear").toString());
    }

    public static int parseWorkFrom(@NonNull Map<String, Object> object) {
        return parseInt(parseValueMap(object, "workFrom").get("workFrom").toString());
    }

    // Person
    public static long parsePersonId(@NonNull Map<String, Object> object) {
        return parseId(parseValueMap(object, "person"));
    }

    public static String parsePersonFirstName(@NonNull Map<String, Object> object) {
        return parseFirstName(parseValueMap(object, "person"));
    }

    public static String parsePersonLastName(@NonNull Map<String, Object> object) {
        return parseLastName(parseValueMap(object, "person"));
    }

    public static String parsePersonLocationIP(@NonNull Map<String, Object> person) {
        return parseStringValue(person, "locationIP");
    }

    public static String parsePersonBrowserUsed(@NonNull Map<String, Object> person) {
        return parseStringValue(person, "browserUsed");
    }

    public static String parsePersonGender(@NonNull Map<String, Object> person) {
        return parseStringValue(person, "gender");
    }

    public static long parsePersonBirthday(@NonNull Map<String, Object> person) {
        return requireNonNull(parseDateValue(person, "birthday")).getTime();
    }

    public static long parsePersonCommentCreationDate(@NonNull Map<String, Object> person) {
        return requireNonNull(parseDateValue(person, "commentCreationDate")).getTime();
    }

    public static long parsePersonFriendshipCreationDate(@NonNull Map<String, Object> person) {
        return requireNonNull(parseDateValue(person, "friendshipCreationDate")).getTime();
    }

    public static String parsePersonContent(@NonNull Map<String, Object> person) {
        return parseContent(person, "messageContent", "messageImageFile");
    }

    // Organization
    public static String parseOrganizationName(@NonNull Map<String, Object> organization) {
        return parseStringValue(parseValueMap(organization, "organisation"), "name");
    }

    public static int parseOrganizationYearWorkFrom(@NonNull Map<String, Object> organization) {
        return parseInt(parseValueMap(organization, "organisationYear").get("workFrom").toString());
    }

    // Message
    public static String parseMessageContent(@NonNull Map<String, Object> object) {
        return parseContent(parseValueMap(object, "message"), "imageFile", "content");
    }

    public static long parseMessageCreationDate(@NonNull Map<String, Object> object) {
        return parseCreationDate(parseValueMap(object, "message"));
    }

    public static long parseMessageId(@NonNull Map<String, Object> object) {
        return parseId(parseValueMap(object, "message"));
    }

    public static String parseFirstName(@NonNull Map<String, Object> person) {
        return parseStringValue(person, "firstName");
    }

    public static String parseLastName(@NonNull Map<String, Object> person) {
        return parseStringValue(person, "lastName");
    }

    public static long parseId(@NonNull Map<String, Object> object) {
        return parseLongValue(object, "id");
    }

    public static long parseCreationDate(@NonNull Map<String, Object> object) {
        return requireNonNull(parseDateValue(object, "creationDate")).getTime();
    }

    public static long parseLikeCreationDate(@NonNull Map<String, Object> object) {
        return ((Date) toMap(parseValueMap(object, "like")).get("creationDate")).getTime();
    }

    public static String parseContent(
            @NonNull Map<String, Object> object,
            @NonNull String expectedKey,
            @NonNull String alternativeKey
    ) {
        final String messageContent = parseStringValue(object, expectedKey);

        if (isEmpty(messageContent)) {
            return parseStringValue(object, alternativeKey);
        }

        return messageContent;
    }

    @Nullable
    private static Date parseDateValue(@NonNull Map<String, Object> map, @NonNull String key) {
        return parseObjectValue(map.get(key), v -> (Date) v, null);
    }

    private static <T> T parseObjectValue(
            @Nullable Object object,
            @NonNull Function<Object, T> mapper,
            @Nullable T defaultValue) {
        return parseList(object)
                .map(mapper)
                .findFirst()
                .orElse(defaultValue);
    }

    @SuppressWarnings("unchecked")
    private static Stream<Object> parseList(@Nullable Object object) {
        return object == null ? Stream.empty() : ((List<Object>) object).stream();
    }

    private static Map<String, Object> parseValueMap(@NonNull Map<String, Object> map, @NonNull String key) {
        return ofNullable(map.get(key))
                .map(GremlinResponseParsers::toMap)
                .orElse(emptyMap());
    }
}
