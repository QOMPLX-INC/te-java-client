/*
 * Copyright 2015-2019 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package examples.com.qomplx.mdtsdb.client;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;

public class Test
{
    public static void main(String[] args) throws Exception
    {
        boolean enableDebugOutput = false;
        MdtsdbClient admClient = null;
        try
        {
            admClient = SwimlaneCases.createUser(enableDebugOutput, args);

            TimestampCases.run(admClient, enableDebugOutput);

            WsCases.run(admClient, enableDebugOutput);

            ManageUserOperations.run(enableDebugOutput, args);

            Streaming.run(admClient, enableDebugOutput);

            SwimlaneCases.runCases(admClient, enableDebugOutput);
            ArrayCases.runCases(admClient, enableDebugOutput);
            IndexCases.runCases(admClient, enableDebugOutput);
            TriggerCases.runCases(admClient, enableDebugOutput);
            BatchSendCases.run(admClient, enableDebugOutput);
        }
        catch (Exception e)
        {
            if (admClient != null)
                SwimlaneCases.deleteUser(admClient, enableDebugOutput, args);

            e.printStackTrace(System.out);
            System.out.println("Test Exception:");
            System.out.println(e.getMessage());
        }
    }

}
