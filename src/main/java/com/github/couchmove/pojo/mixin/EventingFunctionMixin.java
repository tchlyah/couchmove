package com.github.couchmove.pojo.mixin;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.*;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.manager.eventing.AsyncEventingFunctionManager;
import com.couchbase.client.java.manager.eventing.EventingFunction;
import com.github.couchmove.exception.CouchmoveException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@JsonDeserialize(using = EventingFunctionMixin.EventingFunctionDeserializer.class)
public interface EventingFunctionMixin {

    class EventingFunctionDeserializer extends JsonDeserializer<EventingFunction> {

        private static final String DECODE_FUNCTION = "decodeFunction";

        @Override
        public EventingFunction deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(p);
            byte[] bytes = Mapper.encodeAsBytes(actualObj);
            try {
                Method decodeFunction = AsyncEventingFunctionManager.class.getDeclaredMethod(DECODE_FUNCTION, byte[].class);
                decodeFunction.setAccessible(true);
                //noinspection PrimitiveArrayArgumentToVarargsMethod
                return (EventingFunction) decodeFunction.invoke(null, bytes);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new CouchmoveException("Unable to decode Eventing Function", e);
            }
        }
    }
}
