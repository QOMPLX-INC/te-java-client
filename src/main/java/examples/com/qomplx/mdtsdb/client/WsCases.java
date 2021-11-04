/*
 * Copyright 2015-2019 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package examples.com.qomplx.mdtsdb.client;

//import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.asynchttpclient.ws.WebSocketListener;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;
import com.qomplx.mdtsdb.client.api.Measurement;

import com.google.gson.JsonArray;

import static com.google.common.base.Preconditions.checkArgument;

public class WsCases
{
    public static void run(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        run(admClient, enableDebugOutput, null, 2, new ArrayList<String>(),
            new CountDownLatch(1),  // insert
            new CountDownLatch(1),  // read data and add trigger
            new CountDownLatch(1),  // insert again
            new CountDownLatch(2)); // read data and notifications
        run(admClient, enableDebugOutput, true, 2, new ArrayList<String>(),
            new CountDownLatch(1),  // insert
            new CountDownLatch(1),  // read data and add trigger
            new CountDownLatch(1),  // insert again
            new CountDownLatch(2)); // read data and notifications

        // keycloack auth for websocket
        run1(admClient, enableDebugOutput, null, 2, new ArrayList<String>(),
            new CountDownLatch(1),  // insert
            new CountDownLatch(1),  // read data and add trigger
            new CountDownLatch(1),  // insert again
            new CountDownLatch(2)); // read data and notifications
        run1(admClient, enableDebugOutput, true, 2, new ArrayList<String>(),
            new CountDownLatch(1),  // insert
            new CountDownLatch(1),  // read data and add trigger
            new CountDownLatch(1),  // insert again
            new CountDownLatch(2)); // read data and notifications 
    }

    public static void run(MdtsdbClient admClient, boolean enableDebugOutput, Boolean async, int N, List<String> received,
                           CountDownLatch inter_latch1, CountDownLatch inter_latch2,
                           CountDownLatch inter_latch3, CountDownLatch latch) throws Exception
    {
        System.out.println();
        System.out.println("websocket...");

        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);

        AsyncHttpClient c = null;
        try
        {
            c = new DefaultAsyncHttpClient();
            WebSocket websocket = c
                .prepareGet(client.wsTargetUrl())
                .addHeader("Authorization", client.wsAuthorizationHeader())
                .execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(
                    new WebSocketListener() {
                        @Override
                        public void onTextFrame(String message, boolean finalFragment, int rsv) {
                            received.add(message);
                            System.out.println("received: " + message);
                            inter_latch1.countDown();
                            inter_latch2.countDown();
                            inter_latch3.countDown();
                            latch.countDown();
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

            run_helper(client, websocket, async, N, received,
                       inter_latch1, inter_latch2, inter_latch3, latch);

            websocket.sendCloseFrame();
        }
        finally
        {
            admClient.deleteAppkey(client.getAppKey());
            if(c != null)
                c.close();
        }
    }

    public static void run1(MdtsdbClient admClient, boolean enableDebugOutput, Boolean async, int N, List<String> received,
                           CountDownLatch inter_latch1, CountDownLatch inter_latch2,
                           CountDownLatch inter_latch3, CountDownLatch latch) throws Exception
    {
        System.out.println();
        System.out.println("websocket...");

        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);

        AsyncHttpClient c = null;
        try
        {
            c = new DefaultAsyncHttpClient();
            WebSocket websocket = c
                .prepareGet(admClient.wsTargetUrl())
                .addHeader("Authorization", admClient.wsAuthorizationHeader())
                .execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(
                    new WebSocketListener() {
                        @Override
                        public void onTextFrame(String message, boolean finalFragment, int rsv) {
                            received.add(message);
                            System.out.println("received: " + message);
                            inter_latch1.countDown();
                            inter_latch2.countDown();
                            inter_latch3.countDown();
                            latch.countDown();
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

            run_helper(client, websocket, async, N, received,
                       inter_latch1, inter_latch2, inter_latch3, latch);

            websocket.sendCloseFrame();
        }
        finally
        {
            admClient.deleteAppkey(client.getAppKey());
            if(c != null)
                c.close();
        }
    }

    public static void run_helper(MdtsdbClient client, WebSocket websocket,
                                  Boolean async, int N, List<String> received,
                                  CountDownLatch inter_latch1, CountDownLatch inter_latch2,
                                  CountDownLatch inter_latch3, CountDownLatch latch) throws Exception
    {
        // data to send
        long unixTime0 = System.currentTimeMillis() / 1000L - 120000;
        JsonArray timeSeries = new JsonArray();
        long tLen = 2, v, d, sgn;
        Random rand = new Random();
        for(long i = unixTime0; i < unixTime0 + tLen; i++)
        {
            d = i - unixTime0;
            timeSeries.add(new Measurement()
                     .time(i)
                     .sensor(0)
                     .field("p1", d)
                     .field("p2", "User" + String.valueOf(d))
                     .sensor(1)
                     .field("p1", d + 1)
                     .field("p2", "User" + String.valueOf(d))
                     .build()
            );
        }

        String query, q1, q2;

        // send
        String tf = client.wsBuildSendData(timeSeries, async);
        //System.out.printf("%s (%s): %s\n", client.wsTargetUrl(), client.wsAuthorizationHeader(), tf);
        websocket.sendTextFrame(tf);
        checkArgument(inter_latch1.await(5, TimeUnit.SECONDS), "timeout");

        // read
        query = String.format("select $0 from %d dur %d end.", unixTime0, tLen);
        websocket.sendTextFrame(client.wsBuildQuery(query, async));

        // add notifications
        query = "trigger \"alias1\" insert $0 do web \"{!now} observe {$sensor} at {$timestamp}\" end.";
        websocket.sendTextFrame(client.wsBuildQuery(query, async));
        checkArgument(inter_latch2.await(5, TimeUnit.SECONDS), "timeout");

        // send
        websocket.sendTextFrame(client.wsBuildSendData(timeSeries, async));
        checkArgument(inter_latch3.await(5, TimeUnit.SECONDS), "timeout");

        // read
        query = String.format("select $0 from %d dur %d end.", unixTime0, tLen);
        websocket.sendTextFrame(client.wsBuildQuery(query, async));

        checkArgument(latch.await(5, TimeUnit.SECONDS), "timeout");
        checkArgument(N == received.size(), String.format("wrong number of responses: %d != %d", N, received.size()));
    }
}
