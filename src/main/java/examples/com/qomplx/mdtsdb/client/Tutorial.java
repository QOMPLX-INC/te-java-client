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

public class Tutorial
{
    public static void main(String[] args) throws Exception
    {
        try
        {
            run(true);
        }
        catch(Exception e)
        {
            System.out.println("Exception:");
            System.out.println(e.getMessage());
        }
    }

    public static void run(boolean enableDebugOutput) throws Exception
    {
        System.out.println("Tutorial:");
        System.out.println("--------");

        // Creating a Super-User
        String builtInKey = "Y2DQZRbhZ8DSoq5vu832UgxQfp6Iw/GLsnwBY/pVzSBRDBMBPFIV3MY6ZBbiAKjw";
        String builtInSecret = "R4wBIZ6/kFb3kjOC8QgM1X7AyFb2QXmyKmC23TZMYAn9VF7RBwlV";
        MdtsdbClient superUser = new MdtsdbClient("127.0.0.1", 8080, "", builtInKey, builtInSecret, new Properties());
        String superUserProps = superUser.query("get_report(\"grant\").").toString();
        System.out.println(superUserProps);

        // Creating a User
        Parse res = new Parse(superUser.newAdminkey("My User"));
        if(!res.isOk())
        {
            throw new MdtsdbException("Cannot create a new user: " + res.getMessage());
        }
        MdtsdbClient user = superUser.newAdmClient(res.getKey(), res.getSecretKey());
        String userProps = user.query("get_report(\"grant\").").toString();
        System.out.println(userProps);

        // Sending Data to Auto-allocated Swimlanes
        String str_multi =
        "[{" +
        "    \"tags\": {" +
        "        \"host\": \"host1\"," +
        "        \"env\": \"Production\"" +
        "    }," +
        "    \"data\": [{" +
        "        \"0\": {" +
        "            \"value\": {\"m\": 0, \"n\": 0}" +
        "        }," +
        "        \"1\": {" +
        "            \"value\": {\"m\": 0, \"n\": 0}" +
        "        }," +
        "        \"ns\": 1559161852" +
        "    }, {" +
        "        \"0\": {" +
        "            \"value\": {\"m\": 0, \"n\": 0}" +
        "        }," +
        "        \"1\": {" +
        "            \"value\": {\"m\": 0, \"n\": 25}" +
        "        }," +
        "        \"ns\": 1559161853" +
        "    }]" +
        "}]";
        JsonArray data = (new JsonParser().parse(str_multi)).getAsJsonArray();
        JsonObject status = user.insert(data);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        // Read data using User wide tags
        String read_data = user.query("seect $ key $:host = \"host1\" format json (array: true) end.").toString();
        System.out.println(read_data);

        // Sending Data to Auto-allocated Swimlanes: Measurements
        String measurementQ =
        "USER ENV (measurements: #{" +
        "    \"my_measurement1\": #{" +
        "        \"opts\": {" +
        "            \"always_report_errors\": true," +
        "            \"buffer_off\": true," +
        "            \"autoclean_off\": true," +
        "            \"auto_label\": true" +
        "        }" +
        "    }" +
        "} end.";
        status = user.query(measurementQ);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        str_multi =
        "[{" +
        "    \"tags\": {" +
        "        \"host\": \"host2\"," +
        "        \"env\": \"Production\"" +
        "    }," +
        "    \"measurement\": \"my_measurement1\"," +
        "    \"data\": [{" +
        "        \"0\": {" +
        "            \"value\": {\"m\": 10, \"n\": 10}" +
        "        }," +
        "        \"1\": {" +
        "            \"value\": {\"m\": 10, \"n\": 10}" +
        "        }," +
        "        \"ns\": 1559161854" +
        "    }, {" +
        "        \"0\": {" +
        "            \"value\": {\"m\": 10, \"n\": 10}" +
        "        }," +
        "        \"1\": {" +
        "            \"value\": {\"m\": 10, \"n\": 125}" +
        "        }," +
        "        \"ns\": 1559161855" +
        "    }]" +
        "}]";
        data = (new JsonParser().parse(str_multi)).getAsJsonArray();
        status = user.insert(data);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        // Read data using User wide tags
        read_data = user.query("select $ key $:host = \"host2\" format json (array: true) end.").toString();
        System.out.println(read_data);

        // Creating a Swimlane
        JsonObject optsData = new JsonObject();
        optsData.addProperty("buffer_off", Boolean.TRUE);
        optsData.addProperty("autoclean_off", Boolean.FALSE);
        JsonObject swimlaneProps = user.newAppkey("my swimlane", optsData);
        res = new Parse(swimlaneProps);
        if(!res.isOk())
        {
            throw new MdtsdbException("Cannot create a new swimlane: " + res.getMessage());
        }
        String appKey = res.getKey(), secretKey = res.getSecretKey();
        MdtsdbClient swimlane = user.newClient(appKey, secretKey);
        String keyProps = swimlane.query(String.format("get_swimlane_opts(\"%s\").", appKey)).toString();
        System.out.println(keyProps);

        // Sending Data
        str_multi = String.format(
        "[{" +
        "    \"key\": \"%s\"," +
        "    \"data\": [{" +
        "        \"0\": {" +
        "            \"value\": {\"m\": 10, \"n\": 10}" +
        "        }," +
        "        \"1\": {" +
        "            \"value\": {\"m\": 10, \"n\": 10}" +
        "        }," +
        "        \"ns\": 1559161854" +
        "    }]" +
        "}]", appKey);
        data = (new JsonParser().parse(str_multi)).getAsJsonArray();
        status = user.insert(data);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        // Read data using User wide tags
        read_data = swimlane.query("select $0-$1 end.").toString();
        System.out.println(read_data);

        // Clean...
        JsonArray childAppKeys = (new Parse(user.query("get_swimlanes()."))).getResultAsJsonArray();
        for (JsonElement childAppKeyObj: childAppKeys) {
            String childAppKey = childAppKeyObj.getAsString();
            System.out.println(childAppKey);
            status = user.deleteAppkey(childAppKey);
            checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");
        }

        status = superUser.deleteAdminkey(user.getAdmKey());
        System.out.println(status);
        checkArgument(Parse.getStatus(status) == 1, "expect valid status in response");

        System.out.println(">>> OK");
    }
}
