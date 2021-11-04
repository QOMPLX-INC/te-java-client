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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.asynchttpclient.ws.WebSocketListener;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;
import com.qomplx.mdtsdb.client.api.MdtsdbException;
import com.qomplx.mdtsdb.client.api.Measurement;
import com.qomplx.mdtsdb.client.api.Parse;
import com.qomplx.mdtsdb.client.api.ParseBodyStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import static com.google.common.base.Preconditions.checkArgument;

public class Streaming
{
    public static void run(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        System.out.println("--------------------");
        System.out.println("Streaming:");
        System.out.println("--------------------");

        long blocks = 2;
        long pts = 10;
        long sensors = 5;
        long dataMode = 1;

        {
            JsonObject opts = new JsonObject();
            opts.addProperty("time_slice", 600);
            opts.addProperty("expire", blocks * pts * 10);

            opts.addProperty("dense", true);
            Streaming.runCases2_opts(enableDebugOutput, admClient, null, opts, dataMode, blocks, sensors, pts);
            System.out.println();

            opts.addProperty("dense", false);
            Streaming.runCases2_opts(enableDebugOutput, admClient, null, opts, dataMode, blocks, sensors, pts);
            System.out.println();
        }

        System.out.println(">>> OK");
    }

    public static void runCases1_opts(boolean enableDebugOutput, MdtsdbClient admClient, MdtsdbClient nonAuthClient,
                                      JsonObject opts, long dataMode, long packs, long sensors, long pts, Boolean async) throws Exception
    {
        // create a new swimlane
        System.out.println("create a swimlane...");
        MdtsdbClient client = Streaming.createSwimlane(admClient, enableDebugOutput, opts);
        System.out.printf("\twriting data: packs %d, sensors %s, pts %d\n", packs, sensors, pts);
        write(client, dataMode, packs, sensors, pts);

        class WsTextFrames {

            private CountDownLatch countDownLatch = new CountDownLatch(1);
            private String text = new String();

            public String recv() throws Exception {
                this.countDownLatch.await(1000, TimeUnit.MILLISECONDS);
                String text = new String();
                synchronized(this) {
                    text = this.text;
                    this.text = new String();
                }
                return text;
            }

            public void frame(String frame) {
                synchronized(this) {
                    this.text += frame;
                }
                this.countDownLatch.countDown();
                this.countDownLatch = new CountDownLatch(1);
            }
        };
        WsTextFrames wsTextFrames = new WsTextFrames();

        AsyncHttpClient c = new DefaultAsyncHttpClient();
        WebSocket websocket = c
            .prepareGet(client.wsTargetUrl())
            .addHeader("Authorization", client.wsAuthorizationHeader())
            .execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(
                new WebSocketListener() {
                    @Override
                    public void onTextFrame(String message, boolean finalFragment, int rsv) {
                        wsTextFrames.frame(message);
                    }

                    @Override
                    public void onOpen(WebSocket websocket) { }

                    @Override
                    public void onClose(WebSocket websocket, int code, String reason) {}

                    @Override
                    public void onError(Throwable t) {
                        System.out.println("websocket error:");
                        t.printStackTrace();
                    }
                }
            ).build()).get();


        JsonArray qs1 = new JsonArray();
        qs1.add(String.format("select $0-$%d end.", sensors));
        qs1.add(String.format("select (incremental_select: false) $0-$%d end.", sensors));
        qs1.add(true);

        JsonArray qsl = new JsonArray();
        qsl.add(qs1);

        String tfq, resp;
        JsonParser parser = new JsonParser();
        JsonObject jresp;

        for (int i = 0; i < qsl.size(); ++i) {
            JsonArray test_opts = qsl.get(i).getAsJsonArray();
            String q1 = test_opts.get(0).getAsString();
            String q2 = test_opts.get(1).getAsString();
            Boolean streamBody = test_opts.get(2).getAsBoolean();

            // query
            tfq = client.wsBuildQuery(q1, async, streamBody);
            websocket.sendTextFrame(tfq);

            // receive
            if (async || streamBody) {
                resp = wsTextFrames.recv();
                System.out.printf("\t---- 0> : %s\n", resp);
                jresp = parser.parse(resp).getAsJsonObject();
                Parse j = new Parse(jresp);
                checkArgument(j.isOk(), j.getMessage());
            }

            resp = wsTextFrames.recv();
            if (async) {
                System.out.printf("\t---- 1> : %s\n", resp);
                ParseBodyStream j = ParseBodyStream.ws_parse(resp);
                j.merge_stream_values();
            } else {
                jresp = parser.parse(resp).getAsJsonObject();
                Parse j = new Parse(jresp);
                checkArgument(j.isOk(), j.getMessage());
            }
        }

        websocket.sendCloseFrame();
        c.close();

        // delete swimlanes
        System.out.println("delete swimlanes...");
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

    public static void runCases2_opts(boolean enableDebugOutput, MdtsdbClient admClient, MdtsdbClient nonAuthClient,
                                      JsonObject opts, long dataMode, long packs, long sensors, long pts) throws Exception
    {
        // create a new swimlane
        System.out.println("create a swimlane...");
        MdtsdbClient client = Streaming.createSwimlane(admClient, enableDebugOutput, opts);
        System.out.printf("\twriting data: packs %d, sensors %s, pts %d\n", packs, sensors, pts);
        write(client, dataMode, packs, sensors, pts);

        JsonArray qs1 = new JsonArray();
        qs1.add(String.format("select $0-$%d end.", sensors)); qs1.add(true); qs1.add("ok");
        JsonArray qsl = new JsonArray();
        qsl.add(qs1);

        for (int i = 0; i < qsl.size(); ++i) {
            JsonArray test_opts = qsl.get(i).getAsJsonArray();
            String q1 = test_opts.get(0).getAsString();
            Boolean streamBody = test_opts.get(1).getAsBoolean();

            System.out.printf("\trun query: %s\n", q1);
            JsonObject jresp = client.query(q1, streamBody);

            if (streamBody) {
                ParseBodyStream j = new ParseBodyStream(jresp);
                JsonArray errors = j.getStreamErrors();
                checkArgument(errors.size() == 0, errors.toString());
                JsonObject js = j.jsonseq_to_json();
                checkArgument(js.get("ok") != null, "unexpected 'j.jsonseq_to_json()' result");
            } else {
                Parse j = new Parse(jresp);
                checkArgument(j.isOk(), j.getMessage());
            }
        }
        // delete swimlanes
        System.out.println("delete swimlanes...");
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

    public static MdtsdbClient createSwimlane(MdtsdbClient admClient, boolean enableDebugOutput, JsonObject opts) throws Exception
    {
        String userDetails = "User";
        JsonObject swimlaneProps = admClient.newAppkey(userDetails, opts);
        Parse results = new Parse(swimlaneProps);

        if (!results.isOk())
        {
            throw new MdtsdbException("Cannot create a new swimlane: " + results.getMessage());
        }

        // find properties of the newly created swimlane
        String clUserDetails = results.getUser();
        checkArgument(clUserDetails.equals(userDetails),
                String.format("expect valid user details %s, get %s", clUserDetails, userDetails));

        // create a client
        String clAppKey      = results.getKey();
        String clSecretKey   = results.getSecretKey();

        MdtsdbClient client = admClient.newClient(clAppKey, clSecretKey);
        if (enableDebugOutput)
            client.enableDebugOutput();

        return client;
    }

    public static long[] write(MdtsdbClient client, long dataMode, long packs, long sensors, long n) throws Exception {
        long t0 = java.lang.System.currentTimeMillis() - n;
        long ti = t0;
        for (long pack = 0; pack < packs; ++pack) {
            JsonArray timeSeries = new JsonArray();
            for (long i = 0; i < n; ++i) {
                Measurement.Builder timeSerie = new Measurement().time(i).builder();
                for (long no = 0; no < sensors; ++no) {
                    timeSerie = timeSerie.sensor(Long.toString(no)).value((i + 1) * (no + 1));
                }
                timeSeries.add(timeSerie.build());
                ti += 1;
            }
            JsonObject status = client.insert(timeSeries);
            Parse st = new Parse(status);
            if (!st.isOk())
            {
                throw new MdtsdbException("Error: " + st.getMessage());
            }
        }
        return new long[]{t0, ti};
    }
}
