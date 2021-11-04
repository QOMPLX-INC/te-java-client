/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.api;

import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class ParseBodyStream
{
    private JsonArray resp = null;


    public ParseBodyStream(JsonArray resp)
    {
        this.resp = resp;
    }

    public ParseBodyStream(JsonObject resp)
    {
        this.resp = new JsonArray();
        this.resp.add(resp);
    }

    public int getParseCount() {
        return this.resp.size();
    }

    public Parse getParse(int i) {
        return new Parse(this.resp.get(i).getAsJsonObject());
    }

    public JsonArray getStreamErrors() {
        JsonArray res = new JsonArray();
        for (int i = 0; i < getParseCount(); ++i) {
            String err = getParse(i).getMessage();
            if (err != null) { res.add(err); }
        }
        return res;
    }

    public JsonObject jsonseq_to_json() {
        JsonArray recs = new JsonArray();
        JsonArray errs = new JsonArray();
        JsonArray postpone = new JsonArray();
        for (int i = 0; i < getParseCount(); ++i) {
           Parse p = getParse(i);
           String uuid = p.getUuid();
           if (uuid != null) {
               postpone.add(uuid);
               continue;
           }
           String err = p.getMessage();
           if (err != null) {
               errs.add(errs);
               continue;
           }
           if (p.isOk()) {
               recs.add(this.resp.get(i));
               continue;
           }
        }
        JsonObject result = new JsonObject();
        if (errs.size() > 0)
            result.add("error", errs);
        else if (postpone.size() > 0)
            result.add("postpone", postpone);
        else
            result.add("ok", recs);
        return result;
    }

    public JsonObject merge_stream_values() {

        if (resp.size() == 1) {
            JsonObject r = resp.get(0).getAsJsonObject();
            return r.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonObject();
        }

        JsonObject jr = jsonseq_to_json();
        if (jr.get("ok") == null) {
            return new JsonObject();
        }

        JsonArray res = new JsonArray();
        JsonArray r = jr.get("ok").getAsJsonArray();
        for (int i = 0; i < r.size(); ++i) {
            JsonObject ri = r.get(i).getAsJsonObject();
            res.add(ri.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonObject());
        }

        JsonObject values = res.get(0).getAsJsonObject();
        for (int i = 1; i < res.size(); ++i) {
            JsonObject d = res.get(i).getAsJsonObject();
            for (Map.Entry<String, JsonElement> entry : d.entrySet()) {
                String alias = entry.getKey();
                JsonElement v = entry.getValue();
                if (values.get(alias) != null) {
                    if (v.isJsonObject())
                        merge_json_helper(values.get(alias).getAsJsonObject(), v.getAsJsonObject());
                    else
                        merge_json_helper(values.get(alias).getAsJsonArray(), v.getAsJsonArray());
                } else if (v.isJsonArray()) {
                    values.add(alias, v);
                }
            }
        }
        return values;
    }

    protected void merge_json_helper(JsonObject obj1, JsonObject obj2) {
        for (Map.Entry<String, JsonElement> entry : obj2.entrySet()) {
            obj1.add(entry.getKey(), entry.getValue());
        }
    }

    protected void merge_json_helper(JsonArray a1, JsonArray a2) {
        for (int i = 0; i < a2.size(); ++i) {
            a1.add(a2.get(i));
        }
    }

    public static ParseBodyStream ws_parse(String resp) {
        String[] lines = resp.split("\00011110");
        JsonArray aresp = new JsonArray();
        JsonParser parser = new JsonParser();
        for (int i = 0; i < lines.length; ++i) {
            aresp.add(parser.parse(lines[i]).getAsJsonObject());
        }
        return new ParseBodyStream(aresp);
    }

}