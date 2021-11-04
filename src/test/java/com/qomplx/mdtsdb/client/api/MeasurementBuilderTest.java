/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */

package com.qomplx.mdtsdb.client.api;

import com.google.gson.JsonObject;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 *
 */
public class MeasurementBuilderTest
{
    @Test
    public void buildData1() throws Exception
    {
        JsonObject data = new Measurement()
                         .sensor(10)
                         .field("p1", "v1")
                         .field("p2", "v2")
                         .sensor(20)
                         .value("sensor_value")
                         .tag("p1", "v1")
                         .tag("p2", "v2")
                         .time(1)
                         .build();
        //System.out.print(String.format("\n%s\n", data.toString()));
        assertTrue(data instanceof JsonObject);
        assertEquals(data.toString(),
            "{\"10\":{\"value\":{\"p1\":\"v1\",\"p2\":\"v2\"}}," +
             "\"20\":{\"tags\":{\"p1\":\"v1\",\"p2\":\"v2\"},\"value\":\"sensor_value\"}," +
             "\"ns\":1}");
        JsonObject data2 = new Measurement()
                         .time(100)
                         .sensor(10)
                         .field("p1", "v1")
                         .field("p2", "v2")
                         .field("p3", 12345)
                         .field("p4", 123.45)
                         .sensor(20)
                         .value(3.14)
                         .tag("p1", "v1")
                         .tag("p2", "v2")
                         .build();
        System.out.print(String.format("\n%s\n", data2.toString()));
        assertEquals(data2.toString(),
            "{\"10\":{\"value\":{\"p1\":\"v1\",\"p2\":\"v2\",\"p3\":12345,\"p4\":123.45}}," +
             "\"20\":{\"tags\":{\"p1\":\"v1\",\"p2\":\"v2\"},\"value\":3.14}," +
             "\"ns\":100}");
        JsonObject data3 = new Measurement()
                         .sensor(10)
                         .field("p1", "v1")
                         .field("p2", "v2")
                         .field("p3", false)
                         .pos(1.23, -50.1)
                         .sensor(20)
                         .value("a")
                         .tag("p1", "v1")
                         .tag("p2", "v2")
                         .tag("p3", true)
                         .pos(-12.3, 5.1)
                         .pos(-123.4, 5.1)
                         .time(1)
                         .build();
        System.out.print(String.format("\n%s\n", data3.toString()));
        assertTrue(data3 instanceof JsonObject);
        assertEquals(data3.toString(),
            "{\"10\":{\"geo\":{\"lat\":1.23,\"lng\":-50.1},\"value\":{\"p1\":\"v1\",\"p2\":\"v2\",\"p3\":false}}," + 
            "\"20\":{\"tags\":{\"p1\":\"v1\",\"p2\":\"v2\",\"p3\":true},\"geo\":{\"lat\":-123.4,\"lng\":5.1},\"value\":\"a\"},\"ns\":1}");
    }
}
