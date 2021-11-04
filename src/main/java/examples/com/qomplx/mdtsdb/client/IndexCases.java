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
import com.qomplx.mdtsdb.client.api.MdtsdbServerException;
import com.qomplx.mdtsdb.client.api.Measurement;
import com.qomplx.mdtsdb.client.api.Parse;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import static com.google.common.base.Preconditions.checkArgument;

public class IndexCases
{
    public static void runCases(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        // create a new swimlane
        System.out.println("create a swimlane...");
        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);

        // check error classes reporting

        JsonObject status;
        System.out.println("check error classes reporting...");
        //status = client.query("ENV seconds, minutes, hours end.");
        //try {
        //    Parse.maybeThrow(status);
        //}
        //catch(MdtsdbServerException e) {
        //    System.out.printf("\tcatch: %s (%d)\n", e.getMessage(), e.getErrorStatus());
        //    checkArgument(e.getErrorStatus() == MdtsdbServerException.ERR_CODE_QL_SYNTAX,
        //        "expect valid error status");
        //}

        String resp, queryText;
        // set indexes
        System.out.println("\tset index fields...");
        resp = client.query("set index $0.p1: first, $1.p3: last, $2.p1: max, $2.p2: min end.").toString();
        System.out.println(resp);

        // build data to send
        long unixTime0 = System.currentTimeMillis() / 1000L - 6000;
        JsonArray timeSeries = new JsonArray();
        long tLen = 3;
        for(long i = unixTime0; i < unixTime0 + tLen; i++)
        {
            timeSeries.add(new Measurement()
                         .time(i)
                         .sensor(0)
                         .field("p1", (i - unixTime0 + 1) * (i - unixTime0 + 1) % 7)
                         .field("p2", "abc")
                         .field("p3", i - unixTime0 + 1)
                         .sensor(1)
                         .field("p1", (i - unixTime0 + 1) % 3)
                         .field("p2", "abc")
                         .field("p3", i - unixTime0 + 2)
                         .field("p4", new Object[]{new String("v1"), new Long(100), new Double(1.2)})
                         .sensor(2)
                         .field("p1", (i - unixTime0 + 1) % 5)
                         .field("p2", "abc")
                         .field("p3", i - unixTime0 + 3)
                         .build()
            );
        }

        // send data
        status = client.insert(timeSeries);
        Parse j = new Parse(status);
        if(!j.isOk())
        {
            throw new MdtsdbException("Error: " + j.getMessage());
        }
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        queryText =
            String.format("select first($0.p1), last($1.p3), max($2.p1), min($2.p2) from %d dur \"%ds\" format json end.",
                          unixTime0, tLen);
        System.out.printf("\tquery events with exact time:\n\t\t%s\n", queryText);
        resp = client.query(queryText).toString();
        System.out.println(resp);

        queryText = String.format("select first($0.p1), last($1.p3), max($2.p1), min($2.p2) format json end.",
                                  unixTime0, tLen);
        System.out.printf("\tquery events without explicit time range (using index):\n\t\t%s\n", queryText);
        resp = client.query(queryText).toString();
        System.out.println(resp);

        // set indexes with errors
        System.out.println("\tset index fields with errors...");
        client.query("set index $0 end.").toString();

        // send data
        client.insert(timeSeries);

        System.out.println("\tgot error/warning messages:");
        status = client.getMessages();
        j = new Parse(status);
        if(!j.isOk())
        {
            throw new MdtsdbException("Error: " + j.getMessage());
        }
        JsonArray msgs = j.getResultAsJsonArray();
        for(JsonElement elem : msgs)
        {
            System.out.println(elem.toString());
        }

        // delete swimlanes
        System.out.println("delete swimlanes...");
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

}
