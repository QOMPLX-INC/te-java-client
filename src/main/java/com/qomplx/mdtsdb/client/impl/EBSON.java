/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;
import com.google.gson.JsonNull;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.io.*;
import java.util.*;
import java.util.zip.*;

import org.bson.BsonWriter;
import org.bson.BsonBinaryWriter;
import org.bson.io.BasicOutputBuffer;
import org.bson.BsonBinaryReader;


public class EBSON
{
    public static byte[] json_to_bson(JsonElement elem) throws IOException, IllegalArgumentException {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        json_to_bson(writer, elem);
        writer.flush();
        return buffer.toByteArray();
    }

    public static void json_to_bson(BsonWriter writer, JsonObject obj) throws IllegalArgumentException {
        writer.writeStartDocument();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            writer.writeName(entry.getKey());
            json_to_bson(writer, entry.getValue());
        }
        writer.writeEndDocument();
    }
    public static void json_to_bson(BsonWriter writer, JsonArray arr) throws IllegalArgumentException {
        writer.writeStartArray();
        for (int i = 0; i < arr.size(); i++) {
            json_to_bson(writer, arr.get(i));
        }
        writer.writeEndArray();
    }
    public static void json_to_bson(BsonWriter writer, JsonElement value) throws IllegalArgumentException {
        if (value.isJsonNull()) {
            writer.writeNull();
        } else if (value.isJsonObject()) {
            json_to_bson(writer, value.getAsJsonObject());
        } else if (value.isJsonArray()) {
            json_to_bson(writer, value.getAsJsonArray());
        } else if (value.isJsonPrimitive()) {
            json_to_bson(writer, value.getAsJsonPrimitive());
        } else {
            throw new IllegalArgumentException("unexpected JsonObject value!");
        }
    }
    public static void json_to_bson(BsonWriter writer, JsonPrimitive value) throws IllegalArgumentException {
        if (value.isBoolean()) {
            writer.writeBoolean(value.getAsBoolean());
        } else if (value.isString()) {
            writer.writeString(value.getAsString());
        } else if (value.isNumber()) {
            json_to_bson(writer, value.getAsNumber());
        } else {
            throw new IllegalArgumentException("unexpected JsonPrimitive value: " + value.toString());
        }
    }
    public static void json_to_bson(BsonWriter writer, Number value) {
        if (value.longValue() == value.doubleValue()) {
            long val = value.longValue();
            if (-2147483648 <= val && val <= 2147483647) {
                writer.writeInt32((int)val);
            } else {
                writer.writeInt64(val);
            }
        } else {
            writer.writeDouble(value.doubleValue());
        }
    }

}
