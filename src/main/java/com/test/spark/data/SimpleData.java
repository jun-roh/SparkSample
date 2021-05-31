package com.test.spark.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SimpleData {
    public List<HashMap<String, Object>> setHashMapData(){
        List<HashMap<String, Object>> result = new ArrayList<>();
        HashMap<String, Object> map = new HashMap<>();
        map.put("idx", 1);
        map.put("national", "한국");
        map.put("value", 123);
        result.add(map);
        HashMap<String, Object> map1 = new HashMap<>();
        map1.put("idx", 2);
        map1.put("national", "미국");
        map1.put("value", 225);
        result.add(map1);
        HashMap<String, Object> map2 = new HashMap<>();
        map2.put("idx", 3);
        map2.put("national", "캐나다");
        map2.put("value", 345);
        result.add(map2);

        return result;
    }
}
