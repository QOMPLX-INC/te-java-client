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

public class TriggerCases
{
    public static void runCases(MdtsdbClient admClient, boolean enableDebugOutput)
            throws Exception
    {
        // simple trigger: read amqp events
        System.out.println("create a swimlane...");
        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);

        String resp, queryText;
        // set indexes
        System.out.println("\tset index fields...");
        resp = client.query("set index $11.p3, $11.p4, $15.p3, $15.p4, $16.p3, $16.p4 end.").toString();
        System.out.println(resp);

        // build data to send
        long unixTime0 = System.currentTimeMillis() / 1000L - 6000;
        JsonArray timeSeries = new JsonArray();
        long tLen = 10, v, d;
        for(long i = unixTime0; i < unixTime0 + tLen; i++)
        {
            d = i - unixTime0;
            v = 100 + d % 15;
            timeSeries.add(new Measurement()
                         .time(i)
                         .sensor(11)
                         .field("p3", v)
                         .field("p4", d)
                         .sensor(15)
                         .field("p3", v)
                         .field("p4", d)
                         .sensor(16)
                         .field("p3", v)
                         .field("p4", d)
                         .build()
            );
        }

        // send data
        JsonObject status = client.insert(timeSeries);
        Parse j = new Parse(status);
        if(!j.isOk())
        {
            throw new MdtsdbException("Error: " + j.getMessage());
        }
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        queryText =
            "env rmq_host: \"localhost\", rmq_port: 5672, rmq_user: \"guest\", rmq_pass: \"guest\",\n" +
            "    rmq_vhost: \"/\", rmq_ex: \"mdtsdb_exchange\", rmq_q: \"mdtsdb_notify\", rmq_routing: \"\"\n" +
            "end,\n" +
            "trigger \"alarm2\"\n"+
            "  insert $11\n" +
            "  having $15, $16\n" +
            "    where\n" +
            "      exist $.p4 == $11.p4 + 5,\n" +
            "      not exist $.p3 between $11.p3 - 10 and $11.p3 + 10\n" +
            "  do amqp \"{!now} collision: sensor {$sensor} at {$timestamp}\"\n" +
            "end.";

        resp = client.query(queryText).toString();
        System.out.println(resp);

        JsonArray timeSeries2 = new JsonArray();
        unixTime0 += 10;
        for(long i = unixTime0; i < unixTime0 + tLen; i++)
        {
            d = i - unixTime0;
            v = 100 + d % 15;
            timeSeries2.add(new Measurement()
                         .time(i)
                         .sensor(11)
                         .field("p3", v)
                         .field("p4", d)
                         .build()
            );
        }

        // send data
        status = client.insert(timeSeries2);
        j = new Parse(status);
        if(!j.isOk())
        {
            throw new MdtsdbException("Error: " + j.getMessage());
        }
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        // delete swimlanes
        System.out.println("delete swimlanes...");
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

}
