/*
 * Copyright 2015-2019 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package examples.com.qomplx.mdtsdb.client;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;
import com.qomplx.mdtsdb.client.api.MdtsdbException;
import com.qomplx.mdtsdb.client.api.Parse;

import com.google.gson.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public class ManageUserOperations
{
    public static void run(boolean enableDebugOutput, String[] credentials) throws Exception
    {
        System.out.println("Manage user operations:");
        System.out.println("-----------------------");

        System.out.println("create an admin client...");
        MdtsdbClient superClient = MdtsdbCredentials.createClientFromMasterProperties(enableDebugOutput, credentials);

        // create a new user
        System.out.println("create a new user...");
        String userDetails = "User1";
        JsonObject resp1 = superClient.newAdminkey(userDetails);
        Parse res1 = new Parse(resp1);

        if(!res1.isOk())
        {
            System.out.printf(">>> Error: %s\n", res1.getMessage());
            return;
        }

        // find properties of the newly created user
        String clUserDetails = res1.getUser();
        checkArgument(clUserDetails.equals(userDetails),
            String.format("expect valid user details %s, get %s", userDetails, clUserDetails));

        String clAdmKey      = res1.getKey();
        String clSecretKey   = res1.getSecretKey();

        // get or create (i.e., get secret key of the same user)
        System.out.println("get or create the user...");
        resp1 = superClient.getOrCreateAdminkey(clAdmKey, userDetails);
        res1 = new Parse(resp1);

        if(!res1.isOk())
        {
            System.out.printf(">>> Error: %s\n", res1.getMessage());
            return;
        }

        checkArgument(res1.getUser().equals(userDetails),
            String.format("expect valid user details %s, get %s", userDetails, clUserDetails));
        checkArgument(res1.getKey().equals(clAdmKey),
            String.format("expect valid adm key %s, get %s", clAdmKey, res1.getKey()));
        checkArgument(res1.getSecretKey().equals(clSecretKey),
            String.format("expect valid secret key %s, get %s", clSecretKey, res1.getSecretKey()));

        // get or create a new user
        String clAdmKey2 = clAdmKey + clAdmKey;
        resp1 = superClient.getOrCreateAdminkey(clAdmKey2, userDetails);
        res1 = new Parse(resp1);

        if(!res1.isOk())
        {
            System.out.printf(">>> Error: %s\n", res1.getMessage());
            return;
        }

        checkArgument(res1.getUser().equals(userDetails),
            String.format("expect valid user details %s, get %s", userDetails, clUserDetails));
        checkArgument(res1.getKey().equals(clAdmKey2),
            String.format("expect valid adm key %s, get %s", clAdmKey, res1.getKey()));

        String clSecretKey2 = res1.getSecretKey();

        // ... and again
        resp1 = superClient.getOrCreateAdminkey(clAdmKey2, userDetails);
        res1 = new Parse(resp1);

        if(!res1.isOk())
        {
            System.out.printf(">>> Error: %s\n", res1.getMessage());
            return;
        }

        checkArgument(res1.getUser().equals(userDetails),
            String.format("expect valid user details %s, get %s", userDetails, clUserDetails));
        checkArgument(res1.getKey().equals(clAdmKey2),
            String.format("expect valid adm key %s, get %s", clAdmKey, res1.getKey()));
        checkArgument(res1.getSecretKey().equals(clSecretKey2),
            String.format("expect valid secret key %s, get %s", clSecretKey2, res1.getSecretKey()));

        // create an administrative clients
        MdtsdbClient admClient = superClient.newAdmClient(clAdmKey, clSecretKey);
        if(enableDebugOutput)
            admClient.enableDebugOutput();
        MdtsdbClient admClient2 = superClient.newAdmClient(clAdmKey2, clSecretKey2);
        if(enableDebugOutput)
            admClient2.enableDebugOutput();

        // check swimlane cases with the newly created administrative client
        SwimlaneCases.runCases(admClient, enableDebugOutput);

        // try to manage the user with a non-authorized administrative client
        System.out.println("check usage of a non-authorized administrative client...");
        JsonObject resp2 = admClient.deleteAdminkey(admClient.getAdmKey());
        Parse res2 = new Parse(resp2);

        if(res2.isOk())
        {
            System.out.printf(">>> Error: expect error response from server, get: %s\n", resp2.toString());
            return;
        }

        // get or create swimlanes
        System.out.println("get or create the swimlane...");
        userDetails = "User";
        JsonObject swimlaneProps = admClient.newAppkey(userDetails);
        Parse results = new Parse(swimlaneProps);

        if(!results.isOk())
        {
            throw new MdtsdbException("Cannot create a new swimlane: " + results.getMessage());
        }

        clUserDetails = results.getUser();
        checkArgument(clUserDetails.equals(userDetails),
                String.format("expect valid user details %s, get %s", clUserDetails, userDetails));

        String clAppKey = results.getKey();
        clSecretKey = results.getSecretKey();

        swimlaneProps = admClient.getOrCreateAppkey(userDetails, clAppKey);
        results = new Parse(swimlaneProps);

        if(!results.isOk())
        {
            throw new MdtsdbException("Cannot create a new swimlane: " + results.getMessage());
        }

        checkArgument(results.getUser().equals(userDetails),
                String.format("expect valid user details %s, get %s", userDetails, results.getUser()));
        checkArgument(results.getKey().equals(clAppKey),
                String.format("expect valid app key %s, get %s", clAppKey, results.getKey()));
        checkArgument(results.getSecretKey().equals(clSecretKey),
                String.format("expect valid secret key %s, get %s", clSecretKey, results.getSecretKey()));

        String clAppKey2 = clAppKey + clAppKey;

        swimlaneProps = admClient2.getOrCreateAppkey(userDetails, clAppKey2);
        results = new Parse(swimlaneProps);

        if(!results.isOk())
        {
            throw new MdtsdbException("Cannot create a new swimlane: " + results.getMessage());
        }

        checkArgument(results.getUser().equals(userDetails),
                String.format("expect valid user details %s, get %s", userDetails, results.getUser()));
        checkArgument(results.getKey().equals(clAppKey2),
                String.format("expect valid app key %s, get %s", clAppKey2, results.getKey()));

        clSecretKey2 = results.getSecretKey();

        swimlaneProps = admClient2.getOrCreateAppkey(userDetails, clAppKey2);
        results = new Parse(swimlaneProps);

        if(!results.isOk())
        {
            throw new MdtsdbException("Cannot create a new swimlane: " + results.getMessage());
        }

        checkArgument(results.getUser().equals(userDetails),
                String.format("expect valid user details %s, get %s", userDetails, results.getUser()));
        checkArgument(results.getKey().equals(clAppKey2),
                String.format("expect valid app key %s, get %s", clAppKey2, results.getKey()));
        checkArgument(results.getSecretKey().equals(clSecretKey2),
                String.format("expect valid secret key %s, get %s", clSecretKey2, results.getSecretKey()));

        // try to get access to the swimlanes from another adm key
        swimlaneProps = admClient.getOrCreateAppkey(userDetails, clAppKey2);
        results = new Parse(swimlaneProps);

        if(results.isOk())
        {
            throw new MdtsdbException("Error: expect error response from server, get: " + results.toString());
        }

        // clean up
        checkArgument(Parse.getStatus(admClient.deleteAppkey(clAppKey)) == 1, "expect valid status in response");
        checkArgument(Parse.getStatus(admClient2.deleteAppkey(clAppKey2)) == 1, "expect valid status in response");

        // delete the user
        System.out.println("delete the user...");
        JsonObject resp3 = superClient.deleteAdminkey(admClient.getAdmKey());
        checkArgument(Parse.getStatus(resp3) == 1, "expect valid status in response");
        resp3 = superClient.deleteAdminkey(admClient2.getAdmKey());
        checkArgument(Parse.getStatus(resp3) == 1, "expect valid status in response");

        System.out.println(">>> OK");
    }

}
