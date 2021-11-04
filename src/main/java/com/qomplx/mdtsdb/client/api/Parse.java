/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.api;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class Parse
{
    private JsonObject resp = null;


    public Parse(JsonObject resp)
    {
        this.resp = resp;
    }

    /**
     * Parse response and check if there are errors.
     *
     * @return true if no error message is present in response, false otherwise
     */
    public boolean isOk()
    {
        JsonElement result = resp.get("error");
        return result == null;
    }

    /**
     * Retrieve an error message
     *
     */
    public String getMessage()
    {
        JsonElement err = resp.get("error");
        return err == null ? null : err.getAsJsonObject().get("message").getAsString();
    }

    /**
     * Throw an exception if error is present in server response
     *
     */
    public static void maybeThrow(JsonObject resp) throws MdtsdbServerException
    {
        JsonElement err = resp.get("error");
        if(err != null)
        {
            JsonObject errObj = err.getAsJsonObject();
            JsonElement code = errObj.get("code"), message = errObj.get("message");
            throw new MdtsdbServerException(code == null ? 9000 : code.getAsInt(), message.getAsString());
        }
    }

    /**
     * Retrieve an uuid of the postponed job
     *
     */
    public String getUuid()
    {
        JsonElement err = resp.get("error");
        if(err != null)
        {
            JsonObject o = err.getAsJsonObject();
            if(!o.get("message").getAsString().equals("the job is postponed"))
                return null;
            JsonObject d = o.get("details").getAsJsonObject();
            return String.valueOf(d.get("uuid").getAsString());
        }
        else
        {
            JsonElement result = resp.get("result");
            if(result != null)
            {
                JsonObject o = result.getAsJsonObject();
                if(o == null || o.get("uuid") == null)
                    return null;
                return String.valueOf(o.get("uuid").getAsString());
            }
            else
                return null;
        }
    }

    /**
     * Retrieve an uuid of the aws s3 download job
     *
     */
    public String getUid()
    {
        return stringField("uid");
    }

    /**
     * Retrieve the result as a string.
     *
     * @return string field value
     */
    public String getResultAsString()
    {
        JsonElement result = resp.get("result");
        return result == null ? null : result.getAsString();
    }

    /**
     * Retrieve the result as a json object.
     *
     * @return result JsonObject
     */
    public JsonObject getResultAsJsonObject()
    {
        JsonElement result = resp.get("result");
        return result == null ? null : result.getAsJsonObject();
    }

    /**
     * Retrieve the result as a json array.
     *
     * @return result JsonArray
     */
    public JsonArray getResultAsJsonArray()
    {
        JsonElement result = resp.get("result");
        return result == null ? null : result.getAsJsonArray();
    }

    /**
     * Retrieve a value of the field from response.
     *
     * @param fieldName a name of the field to retrieve
     *
     * @return string field value
     */
    public String stringField(String fieldName)
    {
        JsonElement result = resp.get("result");
        return result == null ? null : result.getAsJsonObject().get(fieldName).getAsString();
    }

    /**
     * Retrieve a value of the field from response.
     *
     * @param fieldName a name of the field to retrieve
     *
     * @return int field value
     */
    public int intField(String fieldName)
    {
        JsonElement result = resp.get("result");
        return result == null ? null : result.getAsJsonObject().get(fieldName).getAsInt();
    }

    /**
     * Retrieve a value of the field from response.
     *
     * @param fieldName a name of the field to retrieve
     *
     * @return double field value
     */
    public double doubleField(String fieldName)
    {
        JsonElement result = resp.get("result");
        return result == null ? null : result.getAsJsonObject().get(fieldName).getAsDouble();
    }

    /**
     * Retrieve a value of the 'key' field from response.
     *
     * @return field value
     */
    public String getKey()
    {
        return stringField("key");
    }

    /**
     * Retrieve a value of the 'secret_key' field from response.
     *
     * @return field value
     */
    public String getSecretKey()
    {
        return stringField("secret_key");
    }

    /**
     * Retrieve a value of the 'user' field from response.
     *
     * @return field value
     */
    public String getUser()
    {
        return stringField("user");
    }

    /**
     * Retrieve a value of the 'status' field from response.
     *
     * @return field value
     */
    public int getStatus()
    {
        return intField("status");
    }

    /**
     * Retrieve a value of the 'index_name' field from response.
     *
     * @return field value
     */
    public String getIndexName()
    {
        return stringField("index_name");
    }

    /**
     * Retrieve a value of the 'type' field from response.
     *
     * @return field value
     */
    public String getTypeName()
    {
        return stringField("type");
    }

    /**
     * Retrieve a value of the 'size' field from response.
     *
     * @return field value
     */
    public int getSize()
    {
        return intField("size");
    }

    /**
     * Retrieve a value of the 'user_key' field from response.
     *
     * @return field value
     */
    public int getUserKey()
    {
        return intField("user_key");
    }

    /**
     * Retrieve a value of the 'geo_scale' field from response.
     *
     * @return field value
     */
    public int getGeoScale()
    {
        return intField("geo_scale");
    }

    /**
     * Retrieve a value of the vector field from response.
     *
     * @param fieldName the field name
     *
     * @return field value
     */
    public List<String> getList(String fieldName)
    {
        JsonElement result = resp.get("result");
        if(result == null)
            return null;

        JsonArray records = result.getAsJsonObject().get(fieldName).getAsJsonArray();
        List<String> values = new ArrayList<String>(records.size());
        for(JsonElement r : records)
        {
            values.add(r.getAsString());
        }

        return values;
    }

    /**
     * Retrieve a value of the 'aggregation' field from response.
     *
     * @return field value
     */
    public List<String> getAggregation()
    {
        return getList("aggregation");
    }

    /**
     * Retrieve a value of the 'labels' field from response.
     *
     * @return field value
     */
    public List<String> getLabels()
    {
        return getList("labels");
    }

    /**
     * Retrieve a value of the 'app_keys' field from response.
     *
     * @return field value
     */
    public List<String> getAppKeys()
    {
        return getList("app_keys");
    }

    /**
     * Retrieve a value of the 'app_key' field from response.
     *
     * @return field value
     */
    public String getAppKey()
    {
        return stringField("app_key");
    }

    /**
     * Retrieve a value of the 'status' field from response.
     *
     * @param resp server response
     *
     * @return field value
     */
    public static int getStatus(JsonObject resp)
    {
        JsonElement result = resp.get("result");
        return result == null ? 0 : result.getAsJsonObject().get("status").getAsInt();
    }

    /**
     * Retrieve typed key details.
     *
     * @return string field value
     */
    public JsonArray getTypedAppkeyStats(String typeName)
    {
        JsonElement result = resp.get("result");
        if(result == null)
            return null;
        return result.getAsJsonObject().get(typeName).getAsJsonArray();
    }

    public static class EventsData
    {
        private int unitStep = 1;
        private String unit = "s";
        private Map<String, Map<Long, JsonElement>> sensors = new LinkedHashMap<String, Map<Long, JsonElement>>();

        public EventsData(JsonObject dataItem)
        {
            this.unitStep = dataItem.get("unit_step").getAsInt();
            this.unit = dataItem.get("unit").getAsString();

            JsonObject values = dataItem.get("values").getAsJsonObject();
            for(Map.Entry<String, JsonElement> entry : values.entrySet())
            {
                JsonObject pointsJson = entry.getValue().getAsJsonObject();
                Map<Long, JsonElement> points = new LinkedHashMap<Long, JsonElement>();

                for(Map.Entry<String, JsonElement> pointJson : pointsJson.entrySet())
                {
                    long second = Long.parseLong(pointJson.getKey());
                    points.put(second, pointJson.getValue());
                }

                sensors.put(entry.getKey(), points);
            }
        }

        /**
         * @return map of sensor values
         */
        public Map<String, Map<Long, JsonElement>> getSensors()
        {
            return sensors;
        }

        /**
         * @return the unit
         */
        public String getUnit()
        {
            return unit;
        }

        /**
         * @return the duration of time interval that holds grouped by time sensor data (1 if no grouping exists)
         */
        public int getUnitStep()
        {
            return unitStep;
        }
    }

    /**
     * @return list of events sensor data, including unit, start time and values
     */
    public List<Parse.EventsData> getEventsData()
    {
        List<Parse.EventsData> r = new ArrayList<Parse.EventsData>();
        JsonObject result = resp.get("result").getAsJsonObject();
        JsonArray data = result.get("data").getAsJsonArray();
        for(JsonElement el : data)
        {
            r.add(new Parse.EventsData(el.getAsJsonObject()));
        }
        return r;
    }
}