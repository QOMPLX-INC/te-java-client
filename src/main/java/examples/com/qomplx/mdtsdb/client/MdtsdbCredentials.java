/*
 * Copyright 2015-2019 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package examples.com.qomplx.mdtsdb.client;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

import com.qomplx.mdtsdb.client.api.MdtsdbClient;
import com.qomplx.mdtsdb.client.api.MdtsdbException;


public class MdtsdbCredentials
{
    private String adminKey  = null;
    private String appKey    = null;
    private String secretKey = null;
    private String host      = null;
    private String port      = null;
    private String useHttps  = null;
    private String compression = null;
    private String compression_level = null;

    public MdtsdbCredentials(InputStream inputStream) throws IOException
    {
        Properties tsProperties = new Properties();

        tsProperties.load(inputStream);
        inputStream.close();

        adminKey  = tsProperties.getProperty("adminKey");
        appKey    = tsProperties.getProperty("appKey");
        secretKey = tsProperties.getProperty("secretKey");
        host      = tsProperties.getProperty("host");
        port      = tsProperties.getProperty("port");
        useHttps  = tsProperties.getProperty("useHttps", "false");
        compression = tsProperties.getProperty("compression", "false");
        compression_level = tsProperties.getProperty("compression_level", "6");
    }

    public String getAdminKey()
    {
        return adminKey;
    }

    public String getAppKey()
    {
        return appKey;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public String getHost()
    {
        return host;
    }

    public String getPort()
    {
        return port;
    }

    protected String getUseHttps()
    {
        return useHttps;
    }

    protected String getCompression()
    {
        return compression;
    }

    protected String getCompressionLevel()
    {
        return compression_level;
    }

    public static MdtsdbClient createClientFromMasterProperties(boolean enableDebugOutput, String[] credentials) throws IOException, MdtsdbException
    {
        return createClientFrom(enableDebugOutput, credentials, "MdtsdbCredentials.Master.properties");
    }

    public static MdtsdbClient createClientFrom(boolean enableDebugOutput, String[] credentials, String name) throws IOException, MdtsdbException
    {
        InputStream inputProps = MdtsdbCredentials.class.getClassLoader().getResourceAsStream(name);
        MdtsdbCredentials tsCredentials = new MdtsdbCredentials(inputProps);

        Properties options = new Properties();
        options.setProperty("useSSL", tsCredentials.getUseHttps());
        options.setProperty("compression", tsCredentials.getCompression());
        options.setProperty("compression_level", tsCredentials.getCompressionLevel());

        MdtsdbClient client = new MdtsdbClient(
            tsCredentials.getHost(),
            Integer.parseInt(tsCredentials.getPort()),
            tsCredentials.getAppKey(),
            tsCredentials.getAdminKey(),
            tsCredentials.getSecretKey(),
            options);

        if (enableDebugOutput)
            client.enableDebugOutput();

        if (credentials.length == 1) {
            String accessToken = credentials[0];
            client.setAccessToken(accessToken, "Bearer");
        } else if (credentials.length == 3) {
            String authUrl = credentials[0];
            String clientId = credentials[1];
            String clientSecret = credentials[2];
            if (authUrl.equalsIgnoreCase("dev") || authUrl.equalsIgnoreCase("qa")) {
                authUrl = String.format("http://qidp.qweb.qos/auth/realms/%s/protocol/openid-connect/token", authUrl);
            }
            client.setAccessCredentials(authUrl, clientId, clientSecret);
        }

        return client;
    }
}
