/*
 * Copyright 2015-2019 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package examples.com.qomplx.mdtsdb.client;

import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.net.URL;
import java.net.URLConnection;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;
import com.qomplx.mdtsdb.client.api.MdtsdbException;
import com.qomplx.mdtsdb.client.api.Measurement;
import com.qomplx.mdtsdb.client.api.Parse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import static com.google.common.base.Preconditions.checkArgument;

public class ArrayCases
{
    public static void runCases(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        // create a new swimlane
        System.out.println("create a swimlane...");
        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);

        // build data to send
        long unixTime0 = System.currentTimeMillis() / 1000L - 1800;
        JsonArray timeSeries = new JsonArray();
        long tLen = 11;
        for(long i = unixTime0; i < unixTime0 + tLen; i++)
        {
            timeSeries.add(new Measurement()
                         .time(i)
                         .sensor(1)
                         .field("p1", new Integer[]{100, 200, 300})
                         .field("p2", new String[]{"v1", "v2", "v3"})
                         .field("p3", new Double[]{1.2, 3.4, 5.6})
                         .field("p4", new Object[]{new String("v1"), new Long(100), new Double(1.2)})
                         .field("p5", new Long[0])
                         .sensor(2)
                         .value(new Integer[]{100, 200, 300})
                         .sensor(3)
                         .value(new String[]{"v1", "v2", "v3"})
                         .sensor(4)
                         .value(new Double[]{1.2, 3.4, 5.6})
                         .sensor(5)
                         .value(new Object[]{new String("v1"), new Long(100), new Double(1.2)})
                         .sensor(6)
                         .value(new Integer[0])
                         .sensor(7)
                         .field("p1", "v1")
                         .field("p2", "v2")
                         .sensor(8)
                         .value("sensor_value")
                         .tag("t1", "v1")
                         .tag("t2", "v2")
                         .sensor(9)
                         .value(10)
                         .sensor(10)
                         .value(20)
                         .build()
            );
        }

        // send events data
        System.out.println("send events data...");
        JsonObject status = client.insert(timeSeries);
        Parse j = new Parse(status);
        if(!j.isOk())
        {
            throw new MdtsdbException("Error: " + j.getMessage());
        }
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        String queryText =
            String.format("select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 from %d dur \"%ds\" format json end.",
                          unixTime0, tLen);
        System.out.printf("\tquery events:\n\t\t%s\n", queryText);
        String resp = client.query(queryText).toString();

        // analyze received events data
        System.out.println("get and check events data...");
        JsonObject respJson = new JsonParser().parse(resp).getAsJsonObject();
        Parse eventsResults = new Parse(respJson);
        List<Parse.EventsData> eventsRecs = eventsResults.getEventsData();
        checkArgument(eventsRecs.size() == 1, "expect one time window in streaming data");

        // retrieve the 1st time window
        Parse.EventsData eventsData = eventsRecs.get(0);
        Map<String, Map<Long, JsonElement>> evSensors = eventsData.getSensors();
        for(Map.Entry<String, Map<Long, JsonElement>> evSensor : evSensors.entrySet())
        {
            // sensor.getKey() holds sensor id, sensor.getValue() is a map of the unix time to the sensor value
            JsonElement elem = timeSeries.get(0).getAsJsonObject().get(evSensor.getKey());
            String sent = ((elem.isJsonObject() && elem.getAsJsonObject().has("value")) ?
                        elem.getAsJsonObject().get("value") : elem).toString();

            Map<Long, JsonElement> receivedSet = evSensor.getValue();
            String received = receivedSet.get(1000000000 * unixTime0).toString();

            checkArgument(sent.equals(received), "sent/received data mismatch");
        }

        // delete swimlanes
        System.out.println("delete swimlanes...");
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

}
