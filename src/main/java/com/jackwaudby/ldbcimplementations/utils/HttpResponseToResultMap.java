package com.jackwaudby.ldbcimplementations.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This script provides a method that converts http response into a result map
 */
@Slf4j
@UtilityClass
public class HttpResponseToResultMap {

    // The response message consists of: requestId, status, result
    // Check code is 200, else return message body
    // If successful get the result
    // result consists of list of maps with property key and the values as a list
    public static HashMap<String, String> httpResponseToResultMap(String httpResponse) {
        JSONObject responseJson = new JSONObject(httpResponse); // convert to JSON
        HashMap<String, String> resultMap = new HashMap<>();
        try {
            JSONObject status = responseJson.getJSONObject("status");               // get response status
            int statusCode = status.getInt("code");                             // get status code
            if (statusCode == 200) {                                                // if HTTP request successful
                JSONArray result = responseJson.getJSONObject("result")
                        .getJSONObject("data").getJSONArray("@value");          // get data

                for (int index = 0; index < result.length(); index++) {             // for each property in list
                    String elementKey = result.getJSONObject(index)
                            .getJSONArray("@value").getString(0);         // get property key
                    String elementValue;
                    try {                                                           // Date/Integer JSON path
                        JSONObject testObject = result.getJSONObject(index)
                                .getJSONArray("@value").getJSONObject(1)
                                .getJSONArray("@value").getJSONObject(0);
                        elementValue = testObject.get("@value").toString();
                    } catch (JSONException e) {                                     // Set/String JSON path
                        int elementValueSize = result.getJSONObject(index)
                                .getJSONArray("@value").getJSONObject(1).
                                getJSONArray("@value").length();
                        if (elementValueSize == 1) {                                // String
                            elementValue = result.getJSONObject(index)
                                    .getJSONArray("@value").getJSONObject(1)
                                    .getJSONArray("@value").getString(0);
                        } else {                                                    // Set
                            ArrayList<String> elementValueSet = new ArrayList<>();
                            for (int i = 0; i < elementValueSize; i++) {
                                elementValueSet.add(result.getJSONObject(index)
                                        .getJSONArray("@value").getJSONObject(1)
                                        .getJSONArray("@value").getString(i));
                            }
                            elementValue = elementValueSet.toString();
                        }
                    }
                    resultMap.put(elementKey, elementValue);                        // add to result map
                }
            } else {                                                                // return error message
                String statusMessage = status.getString("message");
                System.out.println("Status message: " + statusMessage);
                resultMap.put("http_error", statusMessage);
            }
        } catch (JSONException e) {
            log.error("Unexpected error", e);
        }

        return resultMap;

    }
}
