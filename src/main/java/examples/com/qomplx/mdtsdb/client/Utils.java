/*
 * Copyright 2015-2019 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package examples.com.qomplx.mdtsdb.client;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import java.io.*;
import java.util.*;


public class Utils
{
    public static JsonObject without(String [] keys, JsonObject obj)
    {
        JsonObject res = new JsonObject();
        List list = Arrays.asList(keys);

        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            if (list.indexOf(entry.getKey()) != -1) continue;
            res.add(entry.getKey(), without(keys, entry.getValue()));
        }
        return res;
    }
    public static JsonArray without(String [] keys, JsonArray arr)
    {
        JsonArray res = new JsonArray();
        for (int i = 0; i < arr.size(); ++i) {
            res.add(without(keys, arr.get(i)));
        }
        return res;
    }
    public static JsonElement without(String [] keys, JsonElement val)
    {
        if (val.isJsonObject()) {
            return without(keys, val.getAsJsonObject());
        } else if (val.isJsonArray()) {
            return without(keys, val.getAsJsonArray());
        }
        return val;
    }
}
