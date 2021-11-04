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
import java.time.*;
import java.time.temporal.ChronoField;
import java.util.stream.*;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;
import com.qomplx.mdtsdb.client.api.MdtsdbException;
import com.qomplx.mdtsdb.client.api.Measurement;
import com.qomplx.mdtsdb.client.api.Parse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import static com.google.common.base.Preconditions.checkArgument;

public class BatchSendCases
{
    public static void run(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        System.out.println("Batch send multi-swimlane data:");
        System.out.println("-------------------------------");

        long t0 = System.currentTimeMillis() / 1000L - 1;

        System.out.println("using app key");
        runCases(enableDebugOutput, admClient, t0, 20, 111, false);
        System.out.println("using adm key");
        runCases(enableDebugOutput, admClient, t0, 20, 111, true);
        System.out.println();

        System.out.println(">>> OK");
    }

    public static void runCases(boolean enableDebugOutput, MdtsdbClient admClient, long t0, int n_swimlane, int n_points, boolean is_admin)
            throws Exception
    {
        MdtsdbClient[] swimlanes1 = new MdtsdbClient[n_swimlane];
        MdtsdbClient[] swimlanes2 = new MdtsdbClient[n_swimlane];
        JsonArray multi = new JsonArray(), data = null;
        JsonObject status, one;

        // per-swimlane send and prepare data for multi-send
        for(int i = 0; i < n_swimlane; i++) {
            swimlanes1[i] = SwimlaneCases.createSwimlane(admClient, enableDebugOutput); // one-send
            data = prepareData(t0, n_points, i);
            status = swimlanes1[i].insert(data);
            checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

            swimlanes2[i] = SwimlaneCases.createSwimlane(admClient, enableDebugOutput); // multi-send
            one = new JsonObject();
            one.addProperty("key", swimlanes2[i].getAppKey());
            one.add("data", data);
            multi.add(one);
        }

        // do multi-send
        if(is_admin) {
            // expect error, only multi-send data format is allowed in admin version of send data
            status = admClient.insert(data);
            checkArgument((new Parse(status)).getMessage().startsWith("wrong input data"), "expect data format error");

            status = admClient.insert(multi);
            checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");
        }
        else
            status = swimlanes2[0].insert(multi);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        // read back to compare one- and multi-send data
        String resp1, resp2, queryText = String.format("select $0 from %d to %d format json end.", t0, t0 + n_points);
        System.out.println(queryText);
        // analyze received events data
        for(int i = 0; i < n_swimlane; i++) {
            resp1 = swimlanes1[i].query(queryText).toString();
            //System.out.println(resp1);
            resp2 = swimlanes2[i].query(queryText).toString();
            //System.out.println(resp2);

            List<String> r1 = parse(resp1, t0);
            List<String> r2 = parse(resp2, t0);
            checkArgument(r1.equals(r2), "one-/multi-send data mismatch");
        }

        // delete swimlanes
        System.out.println("delete swimlanes...");
        for(int i = 0; i < n_swimlane; i++) {
            admClient.deleteAppkey(swimlanes1[i].getAppKey());
            admClient.deleteAppkey(swimlanes2[i].getAppKey());
        }
    }

    public static JsonArray prepareData(long unixTime0, int tLen, int no)
            throws Exception
    {
        // build data to send
        JsonArray timeSeries = new JsonArray();
        for(long i = unixTime0; i < unixTime0 + tLen; i++)
        {
            timeSeries.add(new Measurement()
                         .time(i)
                         .sensor(0)
                         .field("p1", i - unixTime0 + no)
                         .build()
            );
        }
        return timeSeries;
    }

    public static List<String> parse(String resp, long t0)
            throws Exception
    {
        JsonObject respJson = new JsonParser().parse(resp).getAsJsonObject();
        Parse eventsResults = new Parse(respJson);
        List<Parse.EventsData> eventsRecs = eventsResults.getEventsData();
        checkArgument(eventsRecs.size() == 1, "expect one time window");

        // retrieve the 1st time window
        Parse.EventsData eventsData = eventsRecs.get(0);
        Map<String, Map<Long, JsonElement>> evSensors = eventsData.getSensors();

        List<String> result = new ArrayList<>();
        for(Map.Entry<String, Map<Long, JsonElement>> evSensor : evSensors.entrySet())
        {
            Map<Long, JsonElement> receivedSet = evSensor.getValue();
            for(Map.Entry<Long, JsonElement> receivedElem : receivedSet.entrySet())
            {
                String received = receivedElem.getValue().toString();//receivedSet.get(1000000000 * t0).toString();
                //System.out.println(received);
                result.add(received);
            }
        }

        return result;
    }
}
