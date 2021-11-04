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

public class SwimlaneCases
{
    public static void runCases(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        // create a new swimlane
        MdtsdbClient client = SwimlaneCases.createSwimlane(admClient, enableDebugOutput);
        // build data to send
        long unixTime = System.currentTimeMillis() / 1000L - 60;
        long tLen = 120;
        JsonObject sensorData = new Measurement()
                         .sensor(1)
                         .value(10)
                         .sensor(2)
                         .value(20)
                         .time(unixTime)
                         .build();

        // send events data
        JsonObject status = client.sendEventsData(sensorData);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        // read data
        String resp = client.query("select $1, $2 from recent \"5m\" end.").toString();
        JsonObject respJson = new JsonParser().parse(resp).getAsJsonObject();
        Parse eventsResults = new Parse(respJson);
        List<Parse.EventsData> eventsRecs = eventsResults.getEventsData();
        checkArgument(eventsRecs.size() == 1, "expect one time window in events data");

        Parse.EventsData eventsData = eventsRecs.get(0);
        Map<String, Map<Long, JsonElement>> evSensors = eventsData.getSensors();
        for(Map.Entry<String, Map<Long, JsonElement>> evSensor : evSensors.entrySet())
        {
            String sent = sensorData.get(evSensor.getKey()).toString();

            Map<Long, JsonElement> receivedSet = evSensor.getValue();
            String received = receivedSet.get(1000000000 * unixTime).toString();

            checkArgument(sent.equals(received), "sent/received data mismatch");
        }

        // delete swimlane
        checkArgument(Parse.getStatus(admClient.deleteAppkey(client.getAppKey())) == 1,
            "expect valid status in response");
    }

    public static MdtsdbClient createUser(boolean enableDebugOutput, String[] credentials) throws Exception
    {
        MdtsdbClient superClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput, credentials);
        String userDetails = "User1";
        JsonObject resp1 = superClient.newAdminkey(userDetails);
        Parse res = new Parse(resp1);
        checkArgument(res.isOk(), String.format("Error: %s", res.getMessage()));

        String clAdmKey      = res.getKey();
        String clSecretKey   = res.getSecretKey();

        MdtsdbClient admClient = superClient.newAdmClient(clAdmKey, clSecretKey);
        if(enableDebugOutput)
            admClient.enableDebugOutput();

        return admClient;
    }

    public static void deleteUser(MdtsdbClient user, boolean enableDebugOutput, String[] credentials) throws Exception
    {
        JsonObject status;
        JsonObject resp = user.query("get_swimlanes().");
        Parse res = new Parse(resp);
        JsonArray childAppKeys = res.getResultAsJsonArray();
        if(childAppKeys != null) {
            for (JsonElement childAppKeyObj: childAppKeys) {
                String childAppKey = childAppKeyObj.getAsString();
                status = user.deleteAppkey(childAppKey);
                checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");
            }
        }

        MdtsdbClient superClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput, credentials);
        superClient.deleteAdminkey(user.getAdmKey());
    }

    public static MdtsdbClient createSwimlane(MdtsdbClient admClient, boolean enableDebugOutput) throws Exception
    {
        String userDetails = "User";
        JsonObject swimlaneProps = admClient.newAppkey(userDetails);
        Parse results = new Parse(swimlaneProps);

        if(!results.isOk())
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
        if(enableDebugOutput)
            client.enableDebugOutput();

        return client;
    }

    public static List<MdtsdbClient> suggestSwimlanes(MdtsdbClient superClient, MdtsdbClient admClient, boolean enableDebugOutput,
                                                      List<String> suggestNames) throws Exception
    {
        String userDetails = "User";
        List<MdtsdbClient> clients = new ArrayList<>();

        for(String name : suggestNames)
        {
            JsonObject swimlaneProps = admClient.newAppkey(userDetails, name);
            Parse results = new Parse(swimlaneProps);

            if(!results.isOk())
            {
                throw new MdtsdbException("Cannot create a new swimlane: " + results.getMessage());
            }

            String clAppKey      = results.getKey();
            String clSecretKey   = results.getSecretKey();

            checkArgument(clAppKey.equals(name),
                    String.format("expect requested application key %s, get %s", name, clAppKey));

            MdtsdbClient client = admClient.newClient(clAppKey, clSecretKey);
            if(enableDebugOutput)
                client.enableDebugOutput();

            clients.add(client);
        }

        return clients;
    }

    public static byte[] readResourceTestData(String fileName) throws IOException, MdtsdbException
    {
        URL u = MdtsdbCredentials.class.getClassLoader().getResource(fileName);

        URLConnection uc = u.openConnection();
        int contentLength = uc.getContentLength();

        InputStream in = new BufferedInputStream(uc.getInputStream());
        byte[] data = new byte[contentLength];

        int bytesRead = 0, offset = 0;
        while (offset < contentLength)
        {
            bytesRead = in.read(data, offset, data.length - offset);
            if (bytesRead == -1)
                break;
            offset += bytesRead;
        }
        in.close();

        if (offset != contentLength)
        {
            throw new MdtsdbException("read resource: only read " + offset +
                                      " bytes; Expected " + contentLength + " bytes");
        }

        return data;
    }

    private static JsonObject waitData(MdtsdbClient client, String uuid) throws MdtsdbException, InterruptedException
    {
        return waitData(client, uuid, 60, 5);
    }

    private static JsonObject waitData(MdtsdbClient client, String uuid, int attempts, int waitsec)
        throws MdtsdbException, InterruptedException
    {
        JsonObject respJson = null;
        for(int i = 0; i < attempts; i++)
        {
            String resp = client.getStored(uuid).toString();
            respJson = new JsonParser().parse(resp).getAsJsonObject();
            Parse r = new Parse(respJson);
            String msg = r.getMessage();
            if(msg != null && msg.equals("not found"))
                Thread.sleep(waitsec * 1000);
            else
                break;
        }
        return respJson;
    }

}
