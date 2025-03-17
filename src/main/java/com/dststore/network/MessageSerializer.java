package com.dststore.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.DeserializationFeature;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.SerializationFeature;

public class MessageSerializer {
    private final ObjectMapper objectMapper;

    public MessageSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        this.objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .allowIfSubType("com.dststore.message")
                .allowIfSubType("com.dststore")
                .build();

        this.objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
        this.objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        this.objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public String serialize(Object message) throws Exception {
        try {
            String serializedMessage = objectMapper.writeValueAsString(message);
            System.out.println("Serialized Message: " + serializedMessage);
            return serializedMessage;
        } catch (Exception e) {
            System.err.println("Error serializing message: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public Object deserialize(String message) throws Exception {
        try {
            System.out.println("Deserializing Message: " + message);
            Object deserializedObject = objectMapper.readValue(message, Object.class);
            System.out.println("Deserialized Object: " + deserializedObject);
            return deserializedObject;
        } catch (Exception e) {
            System.err.println("Error deserializing message: " + e.getMessage());
            e.printStackTrace();
            
            // Fallback: If direct deserialization fails, try to extract the class name
            // and create an instance using reflection
            try {
                Map<String, Object> jsonMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                String className = (String) jsonMap.get("@class");
                if (className != null) {
                    Class<?> clazz = Class.forName(className);
                    try {
                        // Try to create an instance using the default constructor
                        return clazz.getDeclaredConstructor().newInstance();
                    } catch (Exception ex) {
                        // If that fails, return a proxy or placeholder
                        System.err.println("Could not instantiate class " + className + ": " + ex.getMessage());
                        return new HashMap<String, Object>() {{
                            put("_class", className);
                            put("_error", "Could not deserialize");
                        }};
                    }
                }
            } catch (Exception ex) {
                System.err.println("Fallback deserialization failed: " + ex.getMessage());
            }
            
            throw e;
        }
    }
} 