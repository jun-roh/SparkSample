package com.test.spark.util;

import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtil {
    public static JSONArray convertListToJson(List<HashMap<String, Object>> mapList) throws JSONException {
        JSONArray jsonArray = new JSONArray();
        for(Map<String, Object> map : mapList){
            jsonArray.put(convertMapToJson(map));
        }
        return jsonArray;
    }

    public static List<String> convertListToJsonStringList (List<HashMap<String, Object>> mapList) throws JSONException {
        List<String> result = new ArrayList<>();
        for(Map<String, Object> map : mapList){
            result.add(convertMapToJson(map).toString());
        }
        return result;
    }

    public static JSONObject convertMapToJson(Map<String, Object> map) throws JSONException {
        JSONObject json = new JSONObject();
        for (Map.Entry<String,Object> entry: map.entrySet()){
            String key = entry.getKey();
            Object value = entry.getValue();
            json.put(key, value);
        }
        return json;
    }


}
