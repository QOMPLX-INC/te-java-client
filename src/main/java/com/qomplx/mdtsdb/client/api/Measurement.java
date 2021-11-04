/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.api;

import java.util.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Representation of a Mdtsdb sensor value
 * Build as: Measurement().sensor(10)
 *                        .field("p1", "v1")
 *                        .field("p2", "v2")
 *                        .sensor(20)
 *                        .value("sensor_value")
 *                        .tag("p1", "v1")
 *                        .tag("p2", "v2")
 *                        .time(1)
 *                        .build()
 *
 */
public class Measurement
{
    private Long time = null;

    public Measurement()
    {
    }

    /**
     * @param t unix second to set
     */
    public Measurement time(long t)
    {
        checkArgument(t >= 0, "wrong unix second");
        this.time = t;
        return this;
    }

    /**
     * Create a new Mdtsdb sensor value builder
     *
     * @param sensorId alias of the sensor
     * @return the Builder to add more Builder calls
     */
    public Builder sensor(String sensorId)
    {
        return new Builder(sensorId, this.time);
    }

    /**
     * Create a new Mdtsdb sensor value builder
     *
     * @param sensorId numerical id (alias) of the sensor
     * @return the Builder to add more Builder calls
     */
    public Builder sensor(long sensorId)
    {
        return new Builder(sensorId, this.time);
    }

    /**
     * Create a new Mdtsdb sensor value builder
     *
     * @return the Builder to add more Builder calls
     */
    public Builder builder()
    {
        return new Builder();
    }

    /**
     * Utilize existing JsonObject holding key-value pairs, passed in as method parameter here, to build the measurement
     * @param payload - the measurement payload
     * @return the builder to add more Builder calls
     */
    public Builder payload(JsonObject payload)
    {
        return new Builder(payload, this.time);
    }

    /**
     * Builder for a new Mdtsdb sensor value
     *
     */
    public static final class Builder
    {
        private enum BuilderScalarType {NULL, INT, FLOAT, STRING, ARRAY, BOOL};

        private JsonObject collection = new JsonObject();

        private String sensorId = null;

        private JsonPrimitive scalarValue = null;
        private JsonArray arrayValue = null;
        private BuilderScalarType scalarType = Builder.BuilderScalarType.NULL;
        private JsonObject payload = new JsonObject();
        private JsonObject tags = new JsonObject();
        private JsonObject pos = null;

        private Long time = null;

        /**
         * @param sensorId numerical id (alias) of the sensor
         */
        Builder(String sensorId, Long t)
        {
            checkArgument(sensorId != null && !sensorId.isEmpty(), "expect sensor identifier");
            this.sensorId = sensorId;
            if(t != null)
                this.time = t;
        }

        /**
         * @param sensorId numerical id (alias) of the sensor
         */
        Builder(long sensorId, Long t)
        {
            checkArgument(sensorId >= 0, "expect sensor identifier");
            this.sensorId = String.valueOf(sensorId);
            if(t != null)
                this.time = t;
        }

        /**
         *
         * @param payload The properties of the data being sent
         * @param t - epoch time
         */
        Builder(JsonObject payload, Long t){
            this.payload = payload;
            if(t !=null)
                this.time = t;
        }

        /**
         *
         */
        Builder()
        {
        }

        /**
         * Define a time of the measurement (the sensor value)
         *
         * @param t unix second
         * @return the Builder instance
         */
        public Builder time(long t)
        {
            checkArgument(t >= 0, "wrong unix second");
            this.time = t;
            return this;
        }

        /**
         * Add an integer value (primitive) to the sensor value
         *
         * @param v a value of the measurement
         * @return the Builder instance
         */
        public Builder value(long v)
        {
            this.scalarValue = new JsonPrimitive(v);
            this.scalarType = Builder.BuilderScalarType.INT;
            this.arrayValue = null;
            return this;
        }

        /**
         * Add a float value (primitive) to the sensor value
         *
         * @param v a value of the measurement
         * @return the Builder instance
         */
        public Builder value(double v)
        {
            checkArgument(!Double.isNaN(v), "expect valid double value");
            this.scalarValue = new JsonPrimitive(v);
            this.scalarType = Builder.BuilderScalarType.FLOAT;
            this.arrayValue = null;
            return this;
        }

        /**
         * Add a boolean value (primitive) to the sensor value
         *
         * @param v a value of the measurement
         * @return the Builder instance
         */
        public Builder value(boolean v)
        {
            this.scalarValue = new JsonPrimitive(v);
            this.scalarType = Builder.BuilderScalarType.BOOL;
            this.arrayValue = null;
            return this;
        }

        /**
         * Add a string value (primitive) to the sensor value
         *
         * @param v a value of the measurement
         * @return the Builder instance
         */
        public Builder value(String v)
        {
            this.scalarValue = new JsonPrimitive(v);
            this.scalarType = Builder.BuilderScalarType.STRING;
            this.arrayValue = null;
            return this;
        }

        /**
         * Add an array value (primitive) to the sensor value
         *
         * @param v a value of the measurement
         * @return the Builder instance
         */
        public <T> Builder value(T[] v)
        {
            this.scalarValue = null;
            this.scalarType = Builder.BuilderScalarType.ARRAY;
            this.arrayValue = new JsonArray();
            for(T elem : v)
            {
                arrayValue.add(newJsonElement(elem));
            }
            return this;
        }

        /**
         * Add a geo-position to the point
         *
         * @param lat latitude to set
         * @param lng longitude to set
         * @return the Builder instance
         */
        public Builder pos(double lat, double lng)
        {
            if(pos != null)
            {
                if(pos.has("lat"))
                    pos.remove("lat");
                if(pos.has("lng"))
                    pos.remove("lng");
            }
            else
                pos = new JsonObject();
            pos.addProperty("lat", lat);
            pos.addProperty("lng", lng);
            return this;
        }

        /**
         * Add a property to the value
         *
         * @param propName the property name
         * @param propValue the property value
         * @return the Builder instance
         */
        public Builder field(String propName, String propValue)
        {
            this.payload.addProperty(propName, propValue);
            return this;
        }

        /**
         * Add a property to the value
         *
         * @param propName the property name
         * @param propValue the property value
         * @return the Builder instance
         */
        public Builder field(String propName, long propValue)
        {
            this.payload.addProperty(propName, propValue);
            return this;
        }

        /**
         * Add a property to the value
         *
         * @param propName the property name
         * @param propValue the property value
         * @return the Builder instance
         */
        public Builder field(String propName, double propValue)
        {
            checkArgument(!Double.isNaN(propValue), "expect valid double value");
            this.payload.addProperty(propName, propValue);
            return this;
        }

        /**
         * Add a property to the value
         *
         * @param propName the property name
         * @param propValue the property value
         * @return the Builder instance
         */
        public Builder field(String propName, boolean propValue)
        {
            this.payload.addProperty(propName, propValue);
            return this;
        }

        /**
         * Add a property to the value
         *
         * @param propName the property name
         * @param propValue the property value
         * @return the Builder instance
         */
        public <T> Builder field(String propName, T[] propValue)
        {
            JsonArray arr = new JsonArray();
            for(T elem : propValue)
            {
                arr.add(newJsonElement(elem));
            }
            this.payload.add(propName, arr);
            return this;
        }


        /**
         * Add several properties to the value
         *
         * @param props the map of properties
         * @return the Builder instance
         */
        public Builder field(Map<String, String> props)
        {
            for(Map.Entry<String, String> prop : props.entrySet())
            {
                this.payload.addProperty(prop.getKey(), prop.getValue());
            }
            return this;
        }

        /**
         * Add a tag to the value
         *
         * @param tagName the tag name
         * @param tagValue the tag value
         * @return the Builder instance
         */
        public Builder tag(String tagName, String tagValue)
        {
            this.tags.addProperty(tagName, tagValue);
            return this;
        }

        /**
         * Add a tag to the value
         *
         * @param tagName the tag name
         * @param tagValue the tag value
         * @return the Builder instance
         */
        public Builder tag(String tagName, long tagValue)
        {
            this.tags.addProperty(tagName, tagValue);
            return this;
        }

        /**
         * Add a tag to the value
         *
         * @param tagName the tag name
         * @param tagValue the tag value
         * @return the Builder instance
         */
        public Builder tag(String tagName, double tagValue)
        {
            checkArgument(!Double.isNaN(tagValue), "expect valid double value");
            this.tags.addProperty(tagName, tagValue);
            return this;
        }

        /**
         * Add a tag to the value
         *
         * @param tagName the tag name
         * @param tagValue the tag value
         * @return the Builder instance
         */
        public Builder tag(String tagName, boolean tagValue)
        {
            this.tags.addProperty(tagName, tagValue);
            return this;
        }

        /**
         * Add several tags to the value
         *
         * @param tags the map of tags
         * @return the Builder instance
         */
        public Builder tag(Map<String, String> tags)
        {
            for(Map.Entry<String, String> tag : tags.entrySet())
            {
                this.tags.addProperty(tag.getKey(), tag.getValue());
            }
            return this;
        }

        /**
         * Flushes the next sensor data into the points collection
         *
         * @param sensorId numerical id (alias) of the sensor
         */
        public Builder sensor(String sensorId)
        {
            if(this.sensorId != null)
            {
                newPoint();

                checkArgument(sensorId != null && !sensorId.isEmpty(), "expect sensor identifier");

                this.sensorId = sensorId;
                this.scalarValue = null;
                this.arrayValue = null;
                this.payload = new JsonObject();
                this.tags = new JsonObject();
                this.pos = null;
            }
            else
            {
                this.sensorId = sensorId;
            }
            return this;
        }

        /**
         * Flushes the next sensor data into the points collection
         *
         * @param sensorId numerical id (alias) of the sensor
         */
        public Builder sensor(long sensorId)
        {
            if(this.sensorId != null)
            {
                newPoint();

                checkArgument(sensorId >= 0, "expect sensor identifier");

                this.sensorId = String.valueOf(sensorId);
                this.scalarValue = null;
                this.arrayValue = null;
                this.payload = new JsonObject();
                this.tags = new JsonObject();
                this.pos = null;
            }
            else
            {
                this.sensorId = String.valueOf(sensorId);
            }
            return this;
        }

        /**
         * Create a new sensor value
         *
         * @return the newly created value
         */
        public JsonObject build()
        {
            checkArgument(this.sensorId != null && !this.sensorId.isEmpty(), "expect sensor identifier");

            newPoint();

            if(this.time != null)
            {
                checkArgument(this.time >= 0, "wrong timestamp");
                this.collection.addProperty("ns", this.time);
            }

            return this.collection;
        }

        private JsonElement newJsonElement(Object obj)
        {
            if(obj == null)
            {
                return JsonNull.INSTANCE;
            }
            else if (obj instanceof Boolean) {
                return new JsonPrimitive((Boolean) obj);
            }
            else if (obj instanceof Number) {
                return new JsonPrimitive((Number) obj);
            }
            else if (obj instanceof String) {
                return new JsonPrimitive((String) obj);
            }
            else {
                return JsonNull.INSTANCE;
            }
        }

        private void newPoint()
        {
            JsonObject data = new JsonObject();
            if(!this.tags.entrySet().isEmpty())
            {
                data.add("tags", this.tags);
            }
            if(this.pos != null)
            {
                data.add("geo", this.pos);
            }
            if(this.scalarValue == null && this.arrayValue == null)
            {
                if(!this.payload.entrySet().isEmpty())
                {
                    data.add("value", this.payload);
                }

                collection.add(this.sensorId, data);
            }
            else if(!this.tags.entrySet().isEmpty())
            {
                switch(this.scalarType)
                {
                    case INT:
                        data.addProperty("value", this.scalarValue.getAsLong());
                        break;
                    case FLOAT:
                        data.addProperty("value", this.scalarValue.getAsDouble());
                        break;
                    case BOOL:
                        data.addProperty("value", this.scalarValue.getAsBoolean());
                        break;
                    case STRING:
                        data.addProperty("value", this.scalarValue.getAsString());
                        break;
                    case NULL:
                        data.add("value", JsonNull.INSTANCE);
                        break;
                    case ARRAY:
                        data.add("value", this.arrayValue);
                        break;
                }

                collection.add(this.sensorId, data);
            }
            else
            {
                switch(this.scalarType)
                {
                    case INT:
                        collection.addProperty(this.sensorId, this.scalarValue.getAsLong());
                        break;
                    case FLOAT:
                        collection.addProperty(this.sensorId, this.scalarValue.getAsDouble());
                        break;
                    case BOOL:
                        collection.addProperty(this.sensorId, this.scalarValue.getAsBoolean());
                        break;
                    case STRING:
                        collection.addProperty(this.sensorId, this.scalarValue.getAsString());
                        break;
                    case NULL:
                        collection.add(this.sensorId, JsonNull.INSTANCE);
                        break;
                    case ARRAY:
                        collection.add(this.sensorId, this.arrayValue);
                        break;
                }
            }
        }
    }
}
