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

public class TimestampCases
{
    public static void run(MdtsdbClient admClient, boolean enableDebugOutput)
            throws Exception
    {
        System.out.println("Timestamp as nanoseconds:");
        System.out.println("--------------------------");

        System.out.println("test using nanoseconds as an explicit timestamp");
        runCases(enableDebugOutput, admClient, true);
        System.out.println("test using server time as an implicit timestamp");
        runCases(enableDebugOutput, admClient, false);
        System.out.println();

        System.out.println(">>> OK");
    }

    public static void runCases(boolean enableDebugOutput, MdtsdbClient admClient, boolean explicit)
            throws Exception
    {
        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);

        String resp, queryText;
        // sensors have stationary postions
        System.out.println("\tset sensors geo positions...");
        client.query("set options position " + IntStream
            .range(0, 100)
            .mapToObj(no -> String.format("$%d as [%d.0, %d.0]", no, no / 10, no - no / 10 * 10))
            .map(Object::toString)
            .collect(Collectors.joining(", ")) + " end.");

        // build data to send
        JsonArray timeSeries = new JsonArray();
        long step, i, j;
        JsonObject status;
        long startts = System.currentTimeMillis() / 1000L - 1;
        for(step = 1; step < 3; step++)
        {
            for(i = 0; i < 10; i++)
            {
                Measurement.Builder b = new Measurement().builder();
                for(j = 0; j < 10; j++)
                {
                    b = b.sensor(i*10 + j)
                         .field("v1", step*i*j)
                         .field("v2", i*10 + j)
                         .tag("t1", i + j);
                }
                Thread.sleep(10);
                if(explicit)
                {
                    long ts = System.currentTimeMillis() * 1000000L;
                    timeSeries.add(b.time(ts).build());
                }
                else
                {
                    status = client.sendEventsData(b.build());
                    checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");
                }
            }
        }
        long finishts = System.currentTimeMillis() / 1000L + 1;
        if(explicit)
        {
            status = client.insert(timeSeries);
            checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");
        }

        System.out.println("\tqueries:");

        System.out.println("\tsearch using time range:");
        queryText = String.format("select $0-$99 from %d TO %d format json end.", startts, finishts + 300);
        System.out.println(queryText);
        resp = client.query(queryText).toString();
        System.out.println(resp);

        System.out.println("\tsearch using known values:");
        queryText = "select $0-$99 where $:t1 between 9 and 11 format json end.";
        System.out.println(queryText);
        resp = client.query(queryText).toString();
        System.out.println(resp);

        // delete swimlanes
        System.out.println("delete swimlanes...");
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

}
