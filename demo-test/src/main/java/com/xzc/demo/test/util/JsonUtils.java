package com.xzc.demo.test.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * 
 * JsonUtils(json 数据的处理类)
 * 
 * @author lennylv
 * @date 2016年4月29日 下午7:01:49
 * @version [1.0, 2016年4月29日]
 * @since version 1.0
 * @see [相关类/方法]
 */
public final class JsonUtils {
    
    // 私有构造
    private JsonUtils() {
        
    }
    
    private static ObjectMapper objectMapper = null;
    
    static {
        // 将objectMapper 设置为全局静态缓存，提高调用效率
        objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }
    
    
    
    /**
     * 
     * getObjectMapper(供外部调用OjbectMapper对象)
     * 
     * @return
     * @return ObjectMapper 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:02:20
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static ObjectMapper getObjectMapper() {
        
        return objectMapper;
    }
    
    
    /**
     * 
     * obj2json(将对象转为json串)
     * 
     * @param obj
     * @return
     * @return String 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:02:32
     * @version [1.0, 2016年4月29日]
     * @throws IOException 
     * @since version 1.0
     */
    public static String obj2json(Object obj){
        
        if (obj == null) {
            return null;
        }
        
        String jsonResult = null;
        
        try {
            jsonResult = objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            
        }
        
        return jsonResult;
    }
    
    /**
     * 
     * getJsonValue(从json串中获取某key对应的值)
     * 
     * @param jsonSrc
     * @param jsonKey
     * @return
     * @return String 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:02:46
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static String getJsonValue(String jsonSrc, String jsonKey) {
        if (StringUtils.isEmpty(jsonSrc) || StringUtils.isEmpty(jsonKey)) {
            return null;
        }
        JsonNode node = JsonUtils.json2obj(jsonSrc, JsonNode.class);
        
        if (node == null) {
            return null;
        }
        
        // 获取jsonKey数据
        JsonNode dataNode = node.get(jsonKey);
        
        if (null == dataNode) {
            return null;
        }
        
        return dataNode.toString();
    }
    
    /**
     * 
     * getJsonAsBool(从json串中获取某key对应的值)
     * 
     * @param jsonSrc
     * @param jsonKey
     * @return
     * @return boolean 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:03:04
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static boolean getJsonAsBool(String jsonSrc, String jsonKey) {
        JsonNode node = JsonUtils.json2obj(jsonSrc, JsonNode.class);
        
        // 获取jsonKey数据
        JsonNode dataNode = node.get(jsonKey);
        
        return dataNode.asBoolean();
    }
    
    /**
     * 
     * json2obj(将json串转为对象)
     * 
     * @param jsonStr
     * @param clazz
     * @return
     * @return T 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:03:29
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static <T> T json2obj(String jsonStr, Class<T> clazz) {
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        
        T t = null;
        
        try {
            t = objectMapper.readValue(jsonStr, clazz);
        } catch (IOException e) {
            
        }
        return t;
    }
    
    /**
     * 
     * obj2node(将一般对象转为jsonNode)
     * 
     * @param obj
     * @return
     * @return JsonNode 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:03:40
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static JsonNode obj2node(Object obj) {
        
        if (null == obj) {
            return null;
        }
        
        JsonNode node = null;
        
        try {
            node = objectMapper.readTree(obj2json(obj));
        } catch (IOException e) {}
        
        return node;
    }
    
    public static <T> T obj2T(Object obj, Class<T> clazz) {
        
        if (null == obj) {
            return null;
        }
        
        T t = null;
        
        try {
            t = objectMapper.readValue(obj2json(obj), clazz);
        } catch (IOException e) {}
        
        return t;
    }
    
    /**
     * 
     * node2obj(将jsonNode转为一般对象)
     * 
     * @param node
     * @param clazz
     * @return
     * @return T 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:03:55
     * @version [1.0, 2016年4月29日]
     * @throws IOException 
     * @since version 1.0
     */
    public static <T> T node2obj(JsonNode node, Class<T> clazz) throws IOException {
        if (null == node) {
            return null;
        }
        
        return json2obj(obj2json(node), clazz);
    }
    
    /**
     * 
     * json2map(将json串转为map对象)
     * 
     * @param jsonStr
     * @param clazz
     * @return
     * @return Map<String,T> 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:04:04
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    @SuppressWarnings("rawtypes")
	public static <T> Map<String, T> json2map(String jsonStr, Class<T> clazz) {
        
        // 入參校驗
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        
        Map<String, Map<String, Object>> map = null;
        try {
            map = objectMapper.readValue(jsonStr, new TypeReference<Map<String, T>>() {});
        } catch (IOException e) {
            
        }
        
        // 非空校验
        if (CollectionUtils.isEmpty((Collection) map)) {
            return null;
        }
        
        Map<String, T> result = new HashMap<String, T>();
        for (Entry<String, Map<String, Object>> entry : map.entrySet()) {
            result.put(entry.getKey(), map2pojo(entry.getValue(), clazz));
        }
        return result;
    }
    
    /**
     * 
     * node2list(jsonnode 转list对象)
     * 
     * @param jsonNode
     * @param clazz
     * @return
     * @return List<T> 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:04:17
     * @version [1.0, 2016年4月29日]
     * @throws IOException 
     * @since version 1.0
     */
    public static <T> List<T> node2list(JsonNode jsonNode, Class<T> clazz) throws IOException {
        if (null == jsonNode || !jsonNode.isArray()) {
            return null;
        }
        
        List<T> tList = new ArrayList<>();
        
        for (Iterator<JsonNode> nodeIt = jsonNode.iterator(); nodeIt.hasNext();) {
            JsonNode node = nodeIt.next();
            T t = node2obj(node, clazz);
            
            tList.add(t);
        }
        
        return tList;
    }
    
    /**
     * 
     * json2mapWithList(字符串转map，并且map的value为list)
     * 
     * @param jsonStr
     * @param clazz
     * @return
     * @return Map<String,List<T>> 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:04:28
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    @SuppressWarnings("rawtypes")
	public static <T> Map<String, List<T>> json2mapWithList(String jsonStr, Class<T> clazz) {
        
        // 入參校驗
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        
        Map<String, Map<String, List<Object>>> map = null;
        try {
            map = objectMapper.readValue(jsonStr, new TypeReference<Map<String, List<T>>>() {});
        } catch (IOException e) {
            
        }
        
        // 非空校验
        if (CollectionUtils.isEmpty((Collection) map)) {
            return null;
        }
        
        Map<String, List<T>> result = new HashMap<>();
        
        for (Entry<String, Map<String, List<Object>>> entry : map.entrySet()) {
            
            List<T> tList = new ArrayList<>();
            
            @SuppressWarnings("unchecked")
            List<Object> obList = (List<Object>) entry.getValue();
            
            for (Object o : obList) {
                T t = objectMapper.convertValue(o, clazz);
                tList.add(t);
            }
            
            result.put(entry.getKey(), tList);
            
        }
        return result;
    }
    
    /**
     * 
     * json2list( 将json串转为list对象)
     * 
     * @param jsonStr
     * @param clazz
     * @return
     * @return List<T> 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:04:47
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static <T> List<T> json2list(String jsonStr, Class<T> clazz) {
        
        // 入參校驗
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        
        List<Map<String, Object>> list = null;
        try {
            list = objectMapper.readValue(jsonStr, new TypeReference<List<T>>() {});
        } catch (IOException e) {
            
        }
        
        // 非空校验
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        
        List<T> result = new ArrayList<T>();
        for (Map<String, Object> map : list) {
            result.add(map2pojo(map, clazz));
        }
        return result;
    }
    
    /**
     * map convert to javaBean
     */
    private static <T> T map2pojo(@SuppressWarnings("rawtypes") Map map, Class<T> clazz) {
        return objectMapper.convertValue(map, clazz);
    }
    
    /**
     * 
     * simpleJson2list(基本数据类型转换成list)
     * 
     * @param jsonStr
     * @param clazz
     * @return
     * @return List<T> 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:04:59
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static <T> List<T> simpleJson2list(String jsonStr, Class<T> clazz) {
        try {
            List<T> list =
                    objectMapper.readValue(jsonStr, objectMapper.getTypeFactory()
                            .constructCollectionType(List.class, clazz));
            return list;
        } catch (JsonParseException e) {
            return null;
        } catch (JsonMappingException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
    }
    
    /**
     * 
     * json2node(json字符转为JsonNode对象)
     * 
     * @param json
     * @return
     * @return JsonNode 返回类型
     * @author lennylv
     * @date 2016年4月29日 下午7:05:10
     * @version [1.0, 2016年4月29日]
     * @since version 1.0
     */
    public static JsonNode json2node(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            return null;
        }
    }
}
