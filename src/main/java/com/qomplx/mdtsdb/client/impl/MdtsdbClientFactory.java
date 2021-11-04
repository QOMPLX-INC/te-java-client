/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.impl;

import java.io.*;
import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;

import static com.google.common.base.Preconditions.checkArgument;

public class MdtsdbClientFactory
{
    /**
     * Creates a Mdtsdb client that solves administrative tasks.
     *
     * @param tsAdmKey administrative key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newAdmClient(String tsAdmKey, String tsSecretKey)
    {
        checkArgument(!tsAdmKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl("", tsAdmKey, tsSecretKey, new Properties());
    }

    /**
     * Creates a Mdtsdb client to create/query data.
     *
     * @param tsAppKey application (swimlane) key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newClient(String tsAppKey, String tsSecretKey)
    {
        checkArgument(!tsAppKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(tsAppKey, "", tsSecretKey, new Properties());
    }

    /**
     * Creates a Mdtsdb client with a custom endpoint that solves administrative tasks.
     *
     * @param tsAdmKey administrative key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newAdmClient(String tsEndpoint, int tsPort, String tsAdmKey, String tsSecretKey)
    {
        checkArgument(!tsEndpoint.isEmpty() && tsPort > 0, "Expect valid end-point");
        checkArgument(!tsAdmKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(tsEndpoint, tsPort, "", tsAdmKey, tsSecretKey, new Properties());
    }

    /**
     * Creates a Mdtsdb client with a custom endpoint to create/query data.
     *
     * @param tsAppKey application (swimlane) key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newClient(String tsEndpoint, int tsPort, String tsAppKey, String tsSecretKey)
    {
        checkArgument(!tsEndpoint.isEmpty() && tsPort > 0, "Expect valid end-point");
        checkArgument(!tsAppKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(tsEndpoint, tsPort, tsAppKey, "", tsSecretKey, new Properties());
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates a Mdtsdb client that solves administrative tasks.
     *
     * @param tsAdmKey administrative key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newAdmClient(String tsAdmKey, String tsSecretKey, Properties options)
    {
        checkArgument(!tsAdmKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl("", tsAdmKey, tsSecretKey, options);
    }

    /**
     * Creates a Mdtsdb client to create/query data.
     *
     * @param tsAppKey application (swimlane) key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newClient(String tsAppKey, String tsSecretKey, Properties options)
    {
        checkArgument(!tsAppKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(tsAppKey, "", tsSecretKey, options);
    }

    /**
     * Creates a Mdtsdb client with a custom endpoint that solves administrative tasks.
     *
     * @param tsAdmKey administrative key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newAdmClient(String tsEndpoint, int tsPort, String tsAdmKey, String tsSecretKey, Properties options)
    {
        checkArgument(!tsEndpoint.isEmpty() && tsPort > 0, "Expect valid end-point");
        checkArgument(!tsAdmKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(tsEndpoint, tsPort, "", tsAdmKey, tsSecretKey, options);
    }

    /**
     * Creates a Mdtsdb client with a custom endpoint to create/query data.
     *
     * @param tsAppKey application (swimlane) key
     * @param tsSecretKey security key
     */
    public static MdtsdbClientImpl newClient(String tsEndpoint, int tsPort, String tsAppKey, String tsSecretKey, Properties options)
    {
        checkArgument(!tsEndpoint.isEmpty() && tsPort > 0, "Expect valid end-point");
        checkArgument(!tsAppKey.isEmpty() && !tsSecretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(tsEndpoint, tsPort, tsAppKey, "", tsSecretKey, options);
    }

}
