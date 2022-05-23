/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.impl;

import java.io.*;
import java.net.*;
import java.net.http.*;
import java.util.concurrent.*;
import java.text.*;
import java.util.*;
import java.nio.charset.*;

import javax.crypto.*;
import javax.crypto.spec.*;
import java.security.*;

import com.google.common.io.CharStreams;

public class CommunicationLayer
{
    private static final SimpleDateFormat rfc822Date = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
    public static final String MDTSDB_AUTH2 = "MDTSDB-HMAC-SHA256 ", MDTSDB_AUTH2_STREAMING = "MDTSDB-STREAMING-HMAC-SHA256 ";

    public enum HttpMethod {GET, HEAD, PUT, DELETE, POST};

    private HttpMethod tsHttpMethod        = null;
    private String     tsContentType       = null;
    private String     tsApiMethod         = null;
    private String     tsSecretKey         = null;
    private String     tsSignatureKey      = null;
    private String     tsScheme            = null;
    private String     tsAccessToken       = null;
    private String     tsAccessTokenType   = null;

    private String  tsEndpoint = "127.0.0.1";
    private int     tsPort     = 8080;
    private String  tsUrl      = "";
    private String  tsPath     = "";
    private boolean useSSL     = false;

    private boolean isDebug = false;

    private boolean isAdminKeyToSign = false;

    public CommunicationLayer(String tsEndpoint, int tsPort, boolean useSSL, boolean isDebug, HttpMethod tsHttpMethod,
                              String tsContentType, String tsPath, String tsSecretKey, String tsSignatureKey,
                              String tsApiMethod, String tsScheme, String tsAccessToken, String tsAccessTokenType) throws Exception
    {
        init(tsEndpoint, tsPort, useSSL, isDebug, tsHttpMethod, tsContentType, tsPath,
             tsSecretKey, tsSignatureKey, tsApiMethod, tsScheme, tsAccessToken, tsAccessTokenType, false);
    }

    public CommunicationLayer(String tsEndpoint, int tsPort, boolean useSSL, boolean isDebug, HttpMethod tsHttpMethod,
                              String tsContentType, String tsPath, String tsSecretKey, String tsSignatureKey,
                              String tsApiMethod, String tsScheme, String tsAccessToken, String tsAccessTokenType,
                              boolean isAdminKeyToSign) throws Exception
    {
        init(tsEndpoint, tsPort, useSSL, isDebug, tsHttpMethod, tsContentType, tsPath,
             tsSecretKey, tsSignatureKey, tsApiMethod, tsScheme, tsAccessToken, tsAccessTokenType, isAdminKeyToSign);
    }

    private void init(String tsEndpoint, int tsPort, boolean useSSL, boolean isDebug, HttpMethod tsHttpMethod,
                              String tsContentType, String tsPath, String tsSecretKey, String tsSignatureKey,
                              String tsApiMethod, String tsScheme, String tsAccessToken, String tsAccessTokenType,
                              boolean isAdminKeyToSign) throws Exception
    {
        this.useSSL = useSSL;
        this.isDebug = isDebug;

        this.tsEndpoint = tsEndpoint;
        this.tsPort = tsPort;
        this.tsUrl = generateTSUrl(this.tsEndpoint, this.tsPort, tsPath);
        this.tsPath = tsPath;

        this.tsHttpMethod = tsHttpMethod;
        this.tsContentType = tsContentType;
        this.tsApiMethod = tsApiMethod;
        this.tsSecretKey = tsSecretKey;
        this.tsSignatureKey = tsSignatureKey;
        this.tsScheme = tsScheme;
        this.tsAccessToken = tsAccessToken;
        this.tsAccessTokenType = tsAccessTokenType;

        this.isAdminKeyToSign = isAdminKeyToSign;

        rfc822Date.setTimeZone(new SimpleTimeZone(0, "GMT"));
    }

    public String generateTSUrl(String tsEndpoint, int tsPort, String path) throws Exception
    {
        String p = this.useSSL ? "https://" : "http://";
        String requestUrl = String.format("%s%s:%d/%s", p, tsEndpoint, tsPort, path);
        return requestUrl;
    }

    public String generateTSUrl(String tsEndpoint, int tsPort, String path, Map<String, String> parameters) throws Exception
    {
        String p = this.useSSL ? "https://" : "http://";
        String requestUrl = String.format("%s%s:%d/%s", p, tsEndpoint, tsPort, path);
        if(requestUrl.length() > 0 && !requestUrl.endsWith("/"))
        {
            requestUrl += "/";
        }

        // add request parameters
        StringBuffer query = new StringBuffer();
        for(Map.Entry<String, String> parameter : parameters.entrySet())
        {
            if(query.length() > 0)
            {
                query.append("&");
            }

            if(parameter.getValue() == null)
            {
                query.append(parameter.getKey());
            }
            else
            {
                query.append(parameter.getKey() + "=" + URLEncoder.encode(parameter.getValue(), "UTF-8"));
            }
        }

        if (query.length() > 0)
        {
            requestUrl += "?" + query;
        }

        return requestUrl;
    }

    public String callApiMethod(byte[] bytes) throws Exception
    {
        return callApiMethod(new LinkedHashMap<String, String>(), bytes);
    }

    public String callApiMethod(Map<String, String> headers, byte[] bytes) throws Exception
    {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String payloadDigest = toHex(md.digest(bytes));

        headers.putIfAbsent("Content-Type", this.tsContentType);
        return callApiMethodImpl(true, headers, bytes, payloadDigest);
    }

    public String callApiMethod(Map<String, String> headers, byte[] bytes, String payloadDigest) throws Exception
    {
        return callApiMethodImpl(true, headers, bytes, payloadDigest);
    }

    public String callApiMethodUnsigned(Map<String, String> headers, byte[] bytes, String payloadDigest) throws Exception
    {
        return callApiMethodImpl(false, headers, bytes, payloadDigest);
    }

    private String callApiMethodImpl(boolean isSigned, Map<String, String> headers,
                                     byte[] bytes, String payloadDigest) throws Exception
    {
        if (!headers.containsKey("Date"))
            headers.put("Date", rfc822Date.format(currentTime()));

        if (!headers.containsKey("Content-Type"))
            headers.put("Content-Type", this.tsContentType);

        // generate request signature for Authorization Header
        if (isSigned)
        {
            if (this.tsAccessToken == null)
            {
                String signature = CommunicationLayer.makeMdtsdbAuthSignature(this.tsApiMethod, this.tsSignatureKey, this.tsSecretKey,
                                                                              this.tsPath, payloadDigest, headers.get("Content-Type"));
                String MdtsdbAuth2 = String.format("%s%s %s %s,%s", MDTSDB_AUTH2, this.tsSignatureKey,
                                                   signature, isAdminKeyToSign ? "a" : "s", this.tsApiMethod);
                headers.put("Authorization", MdtsdbAuth2);
            }
            else
            {
                String tokenType = this.tsAccessTokenType == null ? "Bearer" : this.tsAccessTokenType;
                headers.put("Authorization", String.format("%s %s", tokenType, this.tsAccessToken));
            }
        }

        int redirectCount = 0;
        while (redirectCount < 4) // repeat requests
        {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(new URI(this.tsUrl))
                .version(HttpClient.Version.HTTP_1_1);

            if(isDebug)
            {
                System.out.println("\n\n========================\n");
                System.out.println("URI: " + (new URI(this.tsUrl)).toString());
                System.out.println("Headers:");
                for(Map.Entry<String, String> header : headers.entrySet())
                    System.out.println("  " + header.getKey() + "=" + header.getValue());
                System.out.println("Body:");
                System.out.println("  " + new String(bytes, StandardCharsets.UTF_8));
            }

            for(Map.Entry<String, String> header : headers.entrySet())
                builder = builder.headers(header.getKey(), header.getValue());

            if(this.tsHttpMethod == HttpMethod.PUT || this.tsHttpMethod == HttpMethod.POST)
                builder = builder.POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
            else
                builder = builder.GET();

            ExecutorService httpExecutor = null;
            HttpClient client = null;

            try
            {
                HttpRequest request = builder.build();

                httpExecutor = Executors.newSingleThreadExecutor();
                client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .executor(httpExecutor)
                    .build();

                //HttpResponse<String> response = client
                //    .send(request, HttpResponse.BodyHandlers.ofString());
                HttpResponse<InputStream> response = client
                    .send(request, HttpResponse.BodyHandlers.ofInputStream());

                int responseCode = response.statusCode();

                if(isDebug)
                {
                    System.out.println("Response Headers:");
                    System.out.println(response.headers().toString());
                    System.out.println("Response Status:");
                    System.out.println(responseCode);
                }

                if(responseCode >= 200 && responseCode < 300)
                {
                    String data = CharStreams.toString(new InputStreamReader(response.body(), "UTF-8"));
                    if(data.length() > 0 && data.charAt(0) == '\u001e') {
                        byte[] cbuf = data.getBytes("US-ASCII");
                        data = new String(cbuf, 1, cbuf.length - 1);
                    }
                    if(isDebug)
                    {
                        System.out.println("Response Body:");
                        System.out.println(data);
                    }
                    return data;
                }
                else
                    throw new Exception("Error: " + responseCode);
            }
            catch(IOException e)
            {
                throw e;
            }
            finally
            {
                if(httpExecutor != null)
                {
                    httpExecutor.shutdownNow();
                }
                if(client != null)
                {
                    client = null;
                }
        }

        }
        throw new IllegalStateException("internal");
    }

    private Date currentTime()
    {
        return new Date(System.currentTimeMillis());
    }

    static public String makeMdtsdbAuthSignature(String method, String signKey, String secretKey, String uri) throws Exception
    {
        return makeMdtsdbAuthSignature(method, signKey, secretKey, uri,
            CommunicationLayer.toHex(MessageDigest.getInstance("SHA-256").digest()), "");
    }

    static public String makeMdtsdbAuthSignature(String method, String signKey, String secretKey,
                                                 String uri, String payloadHash, String tsContentType) throws Exception
    {
        long unixTime = System.currentTimeMillis() / 1000L / 1000L;
        String ts = String.valueOf(unixTime);

        StringBuffer requestStr = new StringBuffer();
        Mac hmac = Mac.getInstance("HmacSHA256");
        byte[] secret1 = secretKey.getBytes("UTF-8");
        hmac.init(new SecretKeySpec(secret1, "HmacSHA256"));
        byte[] secret2 = hmac.doFinal(ts.getBytes("UTF-8"));
        hmac.reset();
        hmac.init(new SecretKeySpec(secret2, "HmacSHA256"));
        byte[] secret3 = hmac.doFinal(method.getBytes("UTF-8"));

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String payloadDigest = toHex(md.digest(String.format("/%s\n%s\n%s", uri, tsContentType, payloadHash).getBytes("UTF-8")));
        String msg = msg = String.format("%s\n%s\n%s", ts, signKey, payloadDigest);

        hmac.reset();
        hmac.init(new SecretKeySpec(secret3, "HmacSHA256"));
        byte[] signature = hmac.doFinal(msg.getBytes("UTF-8"));
        return toHex(signature);
    }

    static public String makeAuthSignature(String method, String signKey, String secretKey) throws Exception
    {
        long unixTime = System.currentTimeMillis() / 1000L / 1000L;

        StringBuffer requestStr = new StringBuffer();
        requestStr.append(String.valueOf(unixTime) + signKey + method);

        // generate signature
        return CommunicationLayer.makeSignature(requestStr.toString(), secretKey);
    }

    static public String makeSignature(String requestStr, String secretKeyStr) throws Exception
    {
        // create an HMAC signing object
        Mac hmac = Mac.getInstance("HmacSHA1");

        // use a secret Key
        SecretKeySpec secretKey = new SecretKeySpec(secretKeyStr.getBytes("UTF-8"), "HmacSHA1");
        hmac.init(secretKey);

        // compute the signature using the HMAC algorithm
        byte[] signature = hmac.doFinal(requestStr.getBytes("UTF-8"));

        // encode the signature bytes into a Base64 string
        return Base64.getEncoder().encodeToString(signature);
    }

    static private String toHex(byte[] bytes)
    {
        StringBuffer result = new StringBuffer();
        for (byte b : bytes)
        {
            String hex = Integer.toHexString(b & 0xff);
            if(hex.length() == 1)
                result.append('0');
            result.append(hex);
        }
        return result.toString();
    }

    protected String getInputStreamAsString(InputStream is) throws IOException {
        StringBuffer responseBody = new StringBuffer();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(is))){
          String line = null;
          while ((line = reader.readLine()) != null){
              responseBody.append(line + "\n");
          }
        }
        return responseBody.toString();
    }
}
