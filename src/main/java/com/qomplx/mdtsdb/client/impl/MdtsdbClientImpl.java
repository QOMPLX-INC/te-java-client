/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.impl;

import java.io.*;
import java.net.*;
import java.net.http.*;
import java.util.concurrent.*;
import java.util.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.security.MessageDigest;
import java.util.stream.Collectors;
import java.util.Base64;

import com.qomplx.mdtsdb.client.api.MdtsdbException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.io.CharStreams;

import org.bson.BsonWriter;
import org.bson.BsonBinaryWriter;
import org.bson.io.BasicOutputBuffer;

public class MdtsdbClientImpl
{
    public enum MdtsdbScheme {
        UNDEFINED(""),
        EVENTS("events"),
        RESULTS("result"),
        KML("kml"),
        GEOEVENTS("geo_events"),
        ASYNC_EVENTS("events");

        private String schemeId;

        MdtsdbScheme(String schemeId){
            this.schemeId = schemeId;
        }

        public String getSchemeId(){
             return schemeId;
        }
    }

    static final String QL = "ql", QL2 = "ql2", WS = "ws", RESULTS = "results";
    static final List<String> AdminMethods = Arrays.asList(
        "newApiKey", "assureApiKey", "deleteApiKey",
        "newAdminKey", "assureAdminKey", "deleteAdminKey");
    static final List<String> CommonDataMethods = Arrays.asList("setData", "ping");

    private String tsAdmKey = null;
    private String tsAppKey = null;
    private String tsSecretKey = null;
    private String tsAccessToken = null;
    private String tsAccessTokenType = null;
    private String tsAuthUrl = null;
    private String tsAuthClientId = null;
    private String tsAuthClientSecret = null;

    private String tsEndpoint = "127.0.0.1";
    private int tsPort = 8080;
    private String tsPath = "";
    Properties options = new Properties();

    private boolean isDebug = false;

    /**
     * Creates a Mdtsdb client with a custom end-point.
     *
     */
    public MdtsdbClientImpl(String tsEndpoint, int tsPort, String tsAppKey, String tsAdmKey, String tsSecretKey, Properties options)
    {
        this.tsAdmKey = tsAdmKey;
        this.tsAppKey = tsAppKey;
        this.tsSecretKey = tsSecretKey;
        this.tsEndpoint = tsEndpoint;
        this.tsPort = tsPort;
        this.options = options;
    }

    /**
     * Creates a Mdtsdb client with a standard end-point.
     *
     */
    public MdtsdbClientImpl(String tsAppKey, String tsAdmKey, String tsSecretKey, Properties options)
    {
        this.tsAdmKey = tsAdmKey;
        this.tsAppKey = tsAppKey;
        this.tsSecretKey = tsSecretKey;
        this.options = options;
    }

    /**
     * Set keycloak access token credentials
     */
    public void setAccessCredentials(String authUrl, String clientId, String clientSecret) throws MdtsdbException
    {
        this.tsAuthUrl = authUrl;
        this.tsAuthClientId = clientId;
        this.tsAuthClientSecret = clientSecret;
        reloadAccessToken();
    }

    /**
     * Reload keycloak access token
     */
    public void reloadAccessToken() throws MdtsdbException
    {
        ExecutorService httpExecutor = null;
        HttpClient httpClient = null;

        try {
            if (this.tsAuthUrl == null)
                throw new IllegalArgumentException("invalid auth url.");
            if (this.tsAuthClientId == null)
                throw new IllegalArgumentException("invalid client id.");
            if (this.tsAuthClientSecret == null)
                throw new IllegalArgumentException("invalid client secret.");

            String postData = String.format("client_id=%s&client_secret=%s&grant_type=client_credentials",
                                            this.tsAuthClientId, this.tsAuthClientSecret);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(this.tsAuthUrl))
                .headers("Content-Type", "application/x-www-form-urlencoded")
                .headers("Cache-Control", "no-cache, no-store, must-revalidate")
                .headers("Pragma", "no-cache")
                .headers("Expires", "0")
                .POST(HttpRequest.BodyPublishers.ofString(postData))
                .build();

            httpExecutor = Executors.newSingleThreadExecutor();
            httpClient = HttpClient
                .newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .executor(httpExecutor)
                .build();

            HttpResponse<String> response = httpClient
                .send(request, HttpResponse.BodyHandlers.ofString());

            int responseCode = response.statusCode();
            if (responseCode == 200) {
                String responseData = response.body();
                // parse reponse
                JsonElement el = new JsonParser().parse(responseData);
                if (el == null)
                    throw new IllegalArgumentException("invalid auth server response: " + responseData);
                JsonObject obj = el.getAsJsonObject();
                JsonElement accessToken = obj.get("access_token");
                JsonElement tokenType = obj.get("token_type");
                if (accessToken == null)
                    throw new IllegalArgumentException("undefined access token");
                if (tokenType == null)
                    tokenType = new JsonPrimitive("Bearer");
                // set access token
                setAccessToken(accessToken.getAsString(), tokenType.getAsString());
            } else {
                throw new IllegalArgumentException("invalid auth server response: " + responseCode);
            }
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }
        finally
        {
            if(httpExecutor != null)
            {
                httpExecutor.shutdownNow();
            }
            if(httpClient != null)
            {
                httpClient = null;
            }
        }
    }

    /**
     * Set keycloak access token
     */
    public void setAccessToken(String accessToken) throws MdtsdbException
    {
        setAccessToken(accessToken, "Bearer");
    }

    /**
     * Set keycloak access token
     */
    public void setAccessToken(String accessToken, String accessTokenType) throws MdtsdbException
    {
        try {
            String[] parts = accessToken.split("\\.");
            if (parts.length < 3)
                throw new IllegalArgumentException("invalid access token.");
            String accessTokenStr = new String(Base64.getDecoder().decode(parts[1]));
            if (accessTokenStr == null)
                throw new IllegalArgumentException("invalid access token.");
            JsonElement el = new JsonParser().parse(accessTokenStr);
            if (el == null)
                throw new IllegalArgumentException("invalid access token.");
            JsonObject obj = el.getAsJsonObject();
            JsonElement mdtsdb_admin_key = obj.get("clientId");
            if (mdtsdb_admin_key == null)
                throw new IllegalArgumentException("unexpected access token: 'clientId' should be specified.");
            if (mdtsdb_admin_key == null)
                this.tsAdmKey = null;
            else
                this.tsAdmKey = mdtsdb_admin_key.getAsString();
            this.tsSecretKey = "";
            this.tsAccessToken = accessToken;
            this.tsAccessTokenType = accessTokenType;
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }
    }

    /**
     * @return the access token
     */
    public String getAccessToken()
    {
        return tsAccessToken;
    }

    /**
     * @return the access token type
     */
    public String getAccessTokenType()
    {
        return tsAccessTokenType;
    }

    /**
     * Enables Debug output.
     *
     */
    public void enableDebugOutput()
    {
        isDebug = true;
    }

    /**
     * @return the administrative key
     */
    public String getAdmKey()
    {
        return tsAdmKey;
    }

    /**
     * @return the application key
     */
    public String getAppKey()
    {
        return tsAppKey;
    }

    /**
     * @return the secret key
     */
    public String getSecretKey()
    {
        return tsSecretKey;
    }

    /**
     * @return the use ssl option
     */
    public boolean getUseSSL() {
        String useSSL = options.getProperty("useSSL", "false");
        return Boolean.parseBoolean(useSSL);
    }

    public String getPath() {
        return tsPath;
    }

    //////////////
    // Factory API

    /**
     * Creates a Mdtsdb client that solves administrative tasks.
     *
     * @param admKey administrative key
     * @param secretKey security key
     */
    public MdtsdbClientImpl newAdmClient(String admKey, String secretKey)
    {
        checkArgument(!admKey.isEmpty() && !secretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(this.tsEndpoint, this.tsPort, "", admKey, secretKey, this.options);
    }

    /**
     * Creates a Mdtsdb client to create/query data.
     *
     * @param appKey application (swimlane) key
     * @param secretKey security key
     */
    public MdtsdbClientImpl newClient(String appKey, String secretKey)
    {
        checkArgument(!appKey.isEmpty() && !secretKey.isEmpty(), "Expect valid API keys");
        return new MdtsdbClientImpl(this.tsEndpoint, this.tsPort, appKey, "", secretKey, this.options);
    }

    ///////////
    // Data API

    /**
     * Uploads data from sensors to server.
     *
     * <p>
     *   Data swimline is determined by the application key.
     *   Several sensor values can be sent at once, so that the SensorData argument
     *   contains JsonObject representation of pairs sensor identifier/sensor value, e.g.
     * </p>
     *
     * <pre>{"ns":1421507439, "0":0,  "1":1,  "2":2}</pre>
     *
     * <p>
     *   Sensor value is either scalar value (numeric or string), or a list of fields of
     *   the json structure encoded in mochijson2:encode() format.
     * </p>
     *
     * <p>
     *   Sensor data to send can be easily built with Measurement utility class.
     * </p>
     *
     * @param sensorData json object, mapping a sensor identifier to the sensor value
     */

    public JsonObject sendEventsData(JsonObject sensorData) throws MdtsdbException
    {
        return sendData(MdtsdbClientImpl.MdtsdbScheme.EVENTS, sensorData);
    }

    /**
     * @see #sendEventsData(JsonObject)
     *
     **/

    public JsonObject sendEventsData(JsonArray sensorData) throws MdtsdbException
    {
        return sendData(MdtsdbClientImpl.MdtsdbScheme.EVENTS, sensorData);
    }

    /**
     * @see #sendEventsData(JsonObject)
     *
     **/

    public JsonObject insert(JsonArray sensorData) throws MdtsdbException
    {
        return sendData(MdtsdbClientImpl.MdtsdbScheme.EVENTS, sensorData);
    }

    /**
     * Uploads geo-data in GeoJSON/TopoJSON/KML format to server.
     *
     * <p>
     *   Data swimline is determined by the application key.
     *   Payload is a string in either GeoJSON, TopoJSON or KML format.
     * </p>
     *
     * <p>
     *   Please see additional details about sent data in README.
     * </p>
     *
     *
     * @param GeojsonOrKml string in either GeoJSON, TopoJSON or KML format
     */

    public JsonObject sendEventsGeoData(String GeojsonOrKml) throws MdtsdbException
    {
        return sendGeoData(MdtsdbClientImpl.MdtsdbScheme.GEOEVENTS, GeojsonOrKml);
    }

    /**
     * Uploads data to server in Keyhole Markup Language (KML/KMZ) format.
     *
     * <p>
     *   Properties argument may hold several key-value records with predefined names to
     *   fill possible gaps in geo-information in KML format on server side.
     * </p>
     *
     * <p>
     *   Available keys for properties are 'id' for sensor identifier, 'ns' for time
     *   (nanosecond and 'value' for sensor value at the given moment of time. All these
     *   records are used in case when server cannot derive such information (id, time or
     *    value) from fields of sent KML data set.
     * </p>
     *
     * @param kmlContent sensor data in Keyhole Markup Language format
     * @param defaultParams maps sensor properties to default values
     *
     */

    public JsonObject uploadKml(String kmlContent, Properties defaultParams) throws MdtsdbException
    {
        JsonObject result = uploadKml_impl(kmlContent, defaultParams);
        if (_check_keycloak_auth_error(result)) {
            result = uploadKml_impl(kmlContent, defaultParams);
        }
        return result;
    }

    protected JsonObject uploadKml_impl(String kmlContent, Properties defaultParams) throws MdtsdbException
    {
        JsonObject result = null;

        try {
            StringBuilder defs = new StringBuilder();
            String v;
            for(String name : Arrays.asList("id", "alias_tag", "ns", "val", "base64", "ms_attr", "ms_tag", "val_tag"))
            {
                v = defaultParams.getProperty(name);
                if(v != null)
                {
                    defs.append(name);
                    defs.append("=");
                    defs.append(URLEncoder.encode(v, "UTF-8"));
                    defs.append("&");
                }
            }

            String q = String.format("%sq=%s&key=%s",
                defs.toString(),
                URLEncoder.encode(kmlContent, "UTF-8"),
                URLEncoder.encode(this.tsAppKey, "UTF-8"));

            CommunicationLayer comLayer = getCommunicationLayer(MdtsdbClientImpl.QL,
                                                    MdtsdbClientImpl.MdtsdbScheme.KML.getSchemeId());

            String data = comLayer.callApiMethod(q.getBytes("UTF-8"));
            result = new JsonParser().parse(data).getAsJsonObject();
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    /**
     * Uploads file to server in Keyhole Markup Language (KML/KMZ) format.
     *
     * <p>
     *   Properties argument may hold several key-value records with predefined names to
     *   fill possible gaps in geo-information in KML format on server side.
     * </p>
     *
     * <p>
     *   Available keys for properties are 'id' for sensor identifier, 'ns' for time
     *   (nanosecond and 'value' for sensor value at the given moment of time. All these
     *   records are used in case when server cannot derive such information (id, time or
     *    value) from fields of sent KML data set.
     * </p>
     *
     * @param filePath a path to the file with sensor data in Keyhole Markup Language format
     * @param defaultParams maps sensor properties to default values
     */

    public JsonObject uploadKmlFile(String filePath, Properties defaultParams) throws MdtsdbException
    {
        try {
            byte[] data = Files.readAllBytes(Paths.get(filePath));
            String kmlContent = new String(data, StandardCharsets.UTF_8);
            return uploadKml(kmlContent, defaultParams);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }
    }

    /**
     * Ping the service.
     *
     * <p>
     *   Server responses with 1 or with an error message if the service is unavailable.
     * </p>
     *
     * @param timeout either maximum number of milliseconds to wait, or null for infinity
     *
     */
    public JsonObject ping(Integer timeout) throws MdtsdbException
    {
        if(timeout == null) {
            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("timeout", "infinity");
            return ping(MdtsdbClientImpl.MdtsdbScheme.EVENTS, paramsData);
        }
        return ping(MdtsdbClientImpl.MdtsdbScheme.EVENTS, timeout);
    }

    ////////////
    // Query API

    /**
     *  Executes a script using the MDTSDB Query Language v1 or v2.
     *
     * @param script query language script content
     * @param version version of the query language
     */

    public JsonObject eventsQuery(String script, Integer version) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.EVENTS, script, version);
    }

    /**
     *  Executes a script using the MDTSDB Query Language v2.
     *
     * @param script query language script content
     */

    public JsonObject query(String script) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.EVENTS, script, 2);
    }

    /**
     *  Executes a script using the MDTSDB Query Language v1 or v2.
     *
     * @param script query language script content
     * @param version version of the query language
     * @param stream stream response body
     */

    public JsonObject eventsQuery(String script, Integer version, Boolean stream) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.EVENTS, script, version, stream);
    }

    /**
     *  Executes a script using the MDTSDB Query Language v2.
     *
     * @param script query language script content
     * @param stream stream response body
     */

    public JsonObject query(String script, Boolean stream) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.EVENTS, script, 2, stream);
    }

    /**
     *  Asynchronously executes a script using the MDTSDB Query Language v1 or v2.
     *
     * @param script query language script content
     * @param version version of the query language
     */

    public JsonObject asyncEventsQuery(String script, Integer version) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.ASYNC_EVENTS, script, version);
    }

    /**
     *  Asynchronously executes a script using the MDTSDB Query Language v1 or v2.
     *
     * @param script query language script content
     */

    public JsonObject asyncQuery(String script) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.ASYNC_EVENTS, script, 2);
    }

    /**
     *  Asynchronously executes a script using the MDTSDB Query Language v1 or v2.
     *
     * @param script query language script content
     * @param version version of the query language
     * @param stream stream response body
     */

    public JsonObject asyncEventsQuery(String script, Integer version, Boolean stream) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.ASYNC_EVENTS, script, version, stream);
    }

    /**
     *  Asynchronously executes a script using the MDTSDB Query Language v2.
     *
     * @param script query language script content
     * @param stream stream response body
     */

    public JsonObject asyncQuery(String script, Boolean stream) throws MdtsdbException
    {
        checkArgument(script != null && !script.isEmpty(), "expect a query to execute");

        return execQuery(MdtsdbClientImpl.MdtsdbScheme.ASYNC_EVENTS, script, 2, stream);
    }

    /**
     *  Queries data, which were stored after delayed execution of the query.
     *
     * @param uuid identifier of the stored data, as returned in details of
     *             the response with notification about delayed execution
     */

    public String getStored(String uuid) throws MdtsdbException
    {
        checkArgument(uuid != null && !uuid.isEmpty(), "expect uuid");

        return delayedQuery(uuid, MdtsdbClientImpl.MdtsdbScheme.RESULTS);
    }

    /**
     *  Queries MDTSDB for Error/Warning diagnostic messages about possible
     *  problems which could happen while data storing and indexing if a user
     *  has defined a list of indexes/incremental aggregation methods and this
     *  list does not conform with the actual data sent to the MDTSDB service.
     */

    public JsonObject getMessages() throws MdtsdbException
    {
        String r = delayedQuery("", MdtsdbClientImpl.MdtsdbScheme.EVENTS);
        return new JsonParser().parse(r).getAsJsonObject();
    }

    ////////////
    // Admin API

    /**
     * Creates a new application key. Requires an admin key.
     *
     * @param comment details of the created user of the application key
     * @param suggestName suggested application key (user is able to select application key if it does not exist)
     *
     */

    public JsonObject newAppkey(String comment, String suggestName) throws MdtsdbException
    {
        return _newOrGetAppkey(comment, -1, null, null, suggestName, null, "newApiKey");
    }

    /**
     * Creates a new application key. Requires an admin key.
     *
     * @param comment details of the created user of the application key
     *
     */

    public JsonObject newAppkey(String comment) throws MdtsdbException
    {
        return _newOrGetAppkey(comment, -1, null, null, null, null, "newApiKey");
    }

    /**
     * Creates a new application key. Requires an admin key.
     *
     * @param comment     details of the created user of the application key
     * @param optsData        swimlane options
     *
     */

    public JsonObject newAppkey(String comment, JsonObject optsData) throws MdtsdbException
    {
        return _newOrGetAppkey(comment, optsData, "newApiKey");
    }

    /**
     * Read secret key of existing application key or creates a new application key. Requires an admin key.
     *
     * <p>
     * Parameter "suggestName" is the application key to get or create.
     * Returns secret key if app key exists and belongs to the admin key that executes the request.
     * Returns error if existing app key belongs to another admin key.
     * Creates a new application key if there is no app key with such name.
     * </p>
     *
     * @param comment details of the created user of the application key
     * @param suggestName suggested application key (user is able to select application key if it does not exist)
     *
     */

    public JsonObject getOrCreateAppkey(String comment, String suggestName) throws MdtsdbException
    {
        return _newOrGetAppkey(comment, -1, null, null, suggestName, null, "assureApiKey");
    }

    /**
     * Read secret key of existing application key or creates a new application key. Requires an admin key.
     *
     * <p>
     * Field "suggest" in the parameter "optsData" is the application key to get or create.
     * Returns secret key if app key exists and belongs to the admin key that executes the request.
     * Returns error if existing app key belongs to another admin key.
     * Creates a new application key if there is no app key with such name.
     * </p>
     *
     * @param comment     details of the created user of the application key
     * @param optsData        swimlane options
     *
     */

    public JsonObject getOrCreateAppkey(String comment, JsonObject optsData) throws MdtsdbException
    {
        return _newOrGetAppkey(comment, optsData, "assureApiKey");
    }

    private JsonObject _newOrGetAppkey(String comment, Integer _dayLimit, Boolean noBufferring, Boolean noCleanOldData,
                                       String suggestName, Integer expireAfter, String apiMethod) throws MdtsdbException
    {
        checkArgument(comment != null, "expect valid user details");
        checkArgument(expireAfter == null || expireAfter > 0, "expect a expire value");
        checkArgument(apiMethod.equals("newApiKey") ||
                      (apiMethod.equals("assureApiKey") && suggestName != null), "expect valid app key in 'suggest' field");

        JsonObject result = null;

        try {
            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("adminkey", this.tsAdmKey);
            paramsData.addProperty("user", comment);

            if(noBufferring != null || noCleanOldData != null || suggestName != null)
            {
                JsonObject optsData = new JsonObject();
                if(noBufferring != null)
                    optsData.addProperty("buffer_off", noBufferring);
                if(noCleanOldData != null)
                    optsData.addProperty("autoclean_off", noCleanOldData);
                if(suggestName != null)
                    optsData.addProperty("suggest", suggestName);
                if(expireAfter != null)
                    optsData.addProperty("expire", expireAfter);
                paramsData.add("opts", optsData);
            }

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private JsonObject _newOrGetAppkey(String comment, JsonObject optsData, String apiMethod) throws MdtsdbException
    {
        checkArgument(comment != null, "expect valid user details");
        checkArgument(optsData != null, "expect valid swimlane options");
        checkArgument(apiMethod.equals("newApiKey") ||
                      (apiMethod.equals("assureApiKey") && optsData.has("suggest")), "expect valid app key in 'suggest' field");

        JsonObject result = null;

        try {
            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("adminkey", this.tsAdmKey);
            paramsData.addProperty("user", comment);
            paramsData.add("opts", optsData);

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    /**
     * Deletes the application key. Requires an admin key.
     *
     * <p>
     *   The administrator key must be the same key that was used to create the application key.
     * </p>
     *
     * @param appKey application key to delete
     *
     */

    public JsonObject deleteAppkey(String appKey) throws MdtsdbException
    {
        return deleteAppkey0(appKey, "deleteApiKey", null);
    }

    /**
     * Deletes the application key. Requires an admin key.
     *
     * <p>
     *   The administrator key must be the same key that was used to create the application key.
     * </p>
     *
     * @param appKey application key to delete
     * @param keepData do not delete data after application key is deleted
     *
     */

    public JsonObject deleteAppkey(String appKey, Boolean keepData) throws MdtsdbException
    {
        return deleteAppkey0(appKey, "deleteApiKey", keepData);
    }

    private JsonObject deleteAppkey0(String appKey, String apiMethod, Boolean keepData) throws MdtsdbException
    {
        checkArgument(appKey != null && !appKey.isEmpty(), "expect valid application key");

        JsonObject result = null;

        try {
            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("key", appKey);
            paramsData.addProperty("adminkey", this.tsAdmKey);
            if(keepData != null)
                paramsData.addProperty("keep_data", keepData);

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    /**
     * Creates a new admin key. Requires an admin key with super-user rights.
     *
     * @param comment details of the created user of the application key
     *
     */

    public JsonObject newAdminkey(String comment) throws MdtsdbException
    {
        checkArgument(comment != null, "expect valid user details");

        JsonObject result = null;

        try {
            String apiMethod = "newAdminKey";

            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("adminkey", this.tsAdmKey);
            paramsData.addProperty("user", comment);

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    /**
     * Read secret key of existing admin key or creates a new admin key. Requires an admin key with super-user rights.
     *
     * <p>
     *   Returns secret key if admin key exists, otherwise creates a new admin key.
     * </p>
     *
     * @param admKey admin key to get or create
     * @param comment details of the created user of the application key
     *
     */

    public JsonObject getOrCreateAdminkey(String admKey, String comment) throws MdtsdbException
    {
        checkArgument(admKey != null, "expect valid admin key to get or create");
        checkArgument(comment != null, "expect valid user details");

        JsonObject result = null;

        try {
            String apiMethod = "assureAdminKey";

            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("adminkey", this.tsAdmKey);
            paramsData.addProperty("suggest", admKey);
            paramsData.addProperty("user", comment);

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    /**
     * Deletes the admin key. Requires an admin key with super-user rights.
     *
     * @param admKey administrative key to delete
     *
     */

    public JsonObject deleteAdminkey(String admKey) throws MdtsdbException
    {
        checkArgument(admKey != null && !admKey.isEmpty(), "expect valid administrative key");

        JsonObject result = null;

        try {
            String apiMethod = "deleteAdminKey";

            JsonObject paramsData = new JsonObject();
            paramsData.addProperty("key", admKey);
            paramsData.addProperty("adminkey", this.tsAdmKey);

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    ///////////////
    // Ws API

    /**
     * Build a target URL for websocket connection.
     *
     */
    public String wsTargetUrl() throws Exception
    {
        return wsTargetUrl(null, null);
    }

    /**
     * Build a target URL for websocket connection.
     *
     * @param async null if async mode is not predefined, true for async mode, false otherwise
     * @param defAppKey set not null if the predefined application key must be set in administrative websocket connection
     */
    public String wsTargetUrl(Boolean async, String defAppKey) throws Exception
    {
        return String.format("ws%s://%s:%d/%s", getUseSSL() ? "s" : "", tsEndpoint, tsPort, wsTargetPath(async, defAppKey));
    }

    /**
     * Build a string with target resource (i.e., path and query from the URL's "scheme:[//authority]path[?query]") for websocket connection.
     *
     * @param async null if async mode is not predefined, true for async mode, false otherwise
     * @param defAppKey set not null if the predefined application key must be set in administrative websocket connection
     */
    public String wsTargetPath(Boolean async, String defAppKey) throws Exception
    {
        if(tsAppKey != null && !tsAppKey.isEmpty())
        {
            String asyncStr = async == null ? "" : (async ? "?async=true" : "?async=false");
            return String.format("api/v1/ws/%s%s", tsAppKey, asyncStr);
        }
        else if(tsAdmKey != null && !tsAdmKey.isEmpty())
        {
            if(defAppKey == null)
            {
                String asyncStr = async == null ? "" : (async ? "?async=true" : "?async=false");
                return String.format("api/v1/ws/%s%s", tsAdmKey, asyncStr);
            }
            else
            {
                String asyncStr = async == null ? "" : (async ? "&async=true" : "&async=false");
                return String.format("api/v1/ws/%s?key=%s%s", tsAdmKey, defAppKey, asyncStr);
            }
        }
        return null;
    }

    /**
     * Build the authorization header for websocket connection.
     *
     */

    public String wsAuthorizationHeader() throws Exception
    {
        return wsAuthorizationHeader(null, null);
    }

    /**
     * Build the authorization header for websocket connection.
     *
     * @param async null if async mode is not predefined, true for async mode, false otherwise
     * @param defAppKey set not null if the predefined application key must be set in administrative websocket connection
     */

    public String wsAuthorizationHeader(Boolean async, String defAppKey) throws Exception
    {
        if (this.tsAccessToken == null)
        {
            String auth = "", userKey = "", info = "s";
            if(tsAppKey != null && !tsAppKey.isEmpty())
            {
                userKey = tsAppKey;
            }
            else if(tsAdmKey != null && !tsAdmKey.isEmpty())
            {
                userKey = tsAdmKey;
                info = "a";
            }
            String path = wsTargetPath(async, defAppKey);
            String signature = CommunicationLayer.makeMdtsdbAuthSignature(MdtsdbClientImpl.WS, userKey, tsSecretKey, path);
            auth = String.format("%s%s %s %s,%s", CommunicationLayer.MDTSDB_AUTH2, userKey, signature, info, MdtsdbClientImpl.WS);
            return auth;
       }
       else
       {
            String tokenType = this.tsAccessTokenType == null ? "Bearer" : this.tsAccessTokenType;
            String auth = String.format("%s %s", tokenType, this.tsAccessToken);
            return auth;
       }
    }

    /**
     * Build a query frame to send using websocket connection.
     *
     * @param query text of the query to execute
     * @param async null if async mode is not predefined, set true for async mode, false otherwise
     * @param streamBody set true for stream body mode, false otherwise
     *
     */

    public String wsBuildQuery(String query, Boolean async, Boolean streamBody) throws Exception
    {
        return wsBuildQuery(MdtsdbClientImpl.MdtsdbScheme.EVENTS, query, async, streamBody);
    }

    /**
     * Build a query frame to send using websocket connection.
     *
     * @param schemeId MdtsdbClientImpl.MdtsdbScheme.EVENTS
     * @param query text of the query to execute
     * @param async set true for async mode, false otherwise
     * @param streamBody set true for stream body mode, false otherwise
     *
     */

    public String wsBuildQuery(MdtsdbClientImpl.MdtsdbScheme schemeId, String query, Boolean async, Boolean streamBody) throws Exception
    {
        checkArgument(query != null, "expect valid query");

        JsonObject paramsData = new JsonObject();
        paramsData.addProperty("q", query);
        paramsData.addProperty("v", "2");
        paramsData.addProperty("stream", (streamBody ? 1 : 0));

        return _wsPostData(schemeId, "q", async, paramsData, null).toString();
    }

    /**
     * Build a query frame to uploads data from sensors to server using websocket connection.
     *
     * @param sensorData json object or array, mapping a sensor identifier to the sensor value
     *
     */

    public String wsBuildSendData(JsonElement sensorData, Boolean async) throws Exception
    {
        return wsBuildSendData(MdtsdbClientImpl.MdtsdbScheme.EVENTS, sensorData, async);
    }

    /**
     * Build a query frame to uploads data from sensors to server using websocket connection.
     *
     * @param schemeId MdtsdbClientImpl.MdtsdbScheme.EVENTS
     * @param sensorData json object or array, mapping a sensor identifier to the sensor value
     * @param async set true for async mode, false otherwise
     *
     */

    public String wsBuildSendData(MdtsdbClientImpl.MdtsdbScheme schemeId, JsonElement sensorData, Boolean async) throws Exception
    {
        checkArgument(sensorData != null, "expect valid sensor data");

        return _wsPostData(schemeId, "setData", async, sensorData, null).toString();
    }

    /**
     * Build a query frame to uploads data to server in Keyhole Markup Language (KML/KMZ) format.
     *
     * <p>
     *   Please see additional details in description of the uploadKml() method.
     * </p>
     *
     * @param kmlContent sensor data in Keyhole Markup Language format
     * @param defaultParams maps sensor properties to default values
     * @param async set true for async mode, false otherwise
     */
    public String wsBuildUploadKml(String kmlContent, Properties defaultParams, Boolean async) throws Exception
    {
        checkArgument(kmlContent != null, "expect valid KML data");
        checkArgument(defaultParams != null, "expect KML upload properies");

        JsonObject opts = new JsonObject();

        String v;
        for(String name : Arrays.asList("id", "alias_tag", "ns", "val", "base64", "ms_attr", "ms_tag", "val_tag"))
        {
            v = defaultParams.getProperty(name);
            if(v != null)
                opts.addProperty(name, URLEncoder.encode(v, "UTF-8"));
        }

        JsonObject paramsData = new JsonObject();
        paramsData.addProperty("q", URLEncoder.encode(kmlContent, "UTF-8"));

        return _wsPostData(MdtsdbClientImpl.MdtsdbScheme.EVENTS, "setData", async, paramsData, opts).toString();
    }

    /**
     * Build a query frame to ping the service for the application key.
     *
     * @param timeout either maximum number of milliseconds to wait, or null for infinity
     * @param async set true for async mode, false otherwise
     *
     */

    public String wsBuildPing(Integer timeout, Boolean async) throws Exception
    {
        return wsBuildPing(MdtsdbClientImpl.MdtsdbScheme.EVENTS, timeout, async);
    }

    /**
     * Build a query frame to ping the service for the application key.
     *
     * @param schemeId MdtsdbClientImpl.MdtsdbScheme.EVENTS
     * @param timeout either maximum number of milliseconds to wait, or null for infinity
     * @param async set true for async mode, false otherwise
     *
     */

    public String wsBuildPing(MdtsdbClientImpl.MdtsdbScheme schemeId, Integer timeout, Boolean async) throws Exception
    {
        JsonObject paramsData = new JsonObject();
        if(timeout == null)
            paramsData.addProperty("timeout", "infinity");
        else
            paramsData.addProperty("timeout", timeout);

        return _wsPostData(schemeId, "ping", async, paramsData, null).toString();
    }

    /**
     *  Build a query frame to query data, which were stored after delayed execution of the query.
     *
     * @param uuid identifier of the stored data, as returned in details of
     *             the response with notification about delayed execution
     * @param async set true for async mode, false otherwise
     *
     */

    public String wsBuildGetStored(String uuid, Boolean async) throws Exception
    {
        checkArgument(uuid != null && !uuid.isEmpty(), "expect uuid");

        return wsBuildDelayedQuery(MdtsdbClientImpl.MdtsdbScheme.RESULTS, uuid, async);
    }

    /**
     * Build a query notification messages.
     *
     * @param async set true for async mode, false otherwise
     *
     */

    public String wsBuildGetMessages(Boolean async) throws Exception
    {
        return wsBuildDelayedQuery(MdtsdbClientImpl.MdtsdbScheme.EVENTS, "", async);
    }

    private String wsBuildDelayedQuery(MdtsdbClientImpl.MdtsdbScheme schemeId, String uuid, Boolean async) throws Exception
    {
        checkArgument(uuid != null, "expect valid uuid");

        JsonObject paramsData = new JsonObject();
        paramsData.addProperty("uuid", uuid);

        return _wsPostData(schemeId, "getResults", async, paramsData, null).toString();
    }

    private JsonObject _wsPostData(MdtsdbClientImpl.MdtsdbScheme schemeId, String apiMethod, Boolean async,
                                   JsonElement paramsData, JsonObject opts)
    {
        JsonObject postData = new JsonObject();

        postData.addProperty("method", apiMethod);
        postData.addProperty("context", schemeId.getSchemeId());
        postData.addProperty("key", this.tsAppKey);

        if(paramsData != null)
            postData.add("params", paramsData);

        if(async != null)
        {
            if(opts == null)
                opts = new JsonObject();
            opts.addProperty("async", async ? 1 : 0);
        }
        if(opts != null)
            postData.add("opts", opts);

        return postData;
    }

    ///////////////
    // Internal API

    public Map<String, String> _call_method_prepare_ep(boolean is_data_ep) {
        Map<String, String> headers = new LinkedHashMap<String, String>();
        if (is_data_ep) {
            String compression = this.options.getProperty("compression", "false");
            if (compression.equals("gzip")) {
                headers.put("Content-Encoding", "gzip");
            } else if (compression.equals("bson")) {
                headers.put("Content-Encoding", "bson");
                headers.put("Content-Type", "application/octet-stream");
            } else if (compression.equals("gzip-bson")) {
                headers.put("Content-Encoding", "gzip-bson");
                headers.put("Content-Type", "application/octet-stream");
            }
        }
        return headers;
    }

    public byte[] _call_method_prepare_content(JsonElement content, boolean is_data_ep) throws IOException, IllegalArgumentException {
        //
        String compression = this.options.getProperty("compression", "false");
        int compression_level = Integer.parseInt(this.options.getProperty("compression_level", "6"));
        if (is_data_ep && compression.equals("gzip")) {
            byte[] utf8 = content.toString().getBytes("UTF-8");
            if (compression_level < 0) { return utf8; }
            if (compression_level > 9) { compression_level = 6; }
            return ErlangTermToBinWrp.compress(utf8, compression_level).array();
        } else if (is_data_ep && compression.equals("bson")) {
            return EBSON.json_to_bson(content);
        } else if (is_data_ep && compression.equals("gzip-bson")) {
            if (compression_level < 0 || compression_level > 9) { compression_level = 6; }
            return ErlangTermToBinWrp.compress(EBSON.json_to_bson(content), compression_level).array();
        } else {
            return content.toString().getBytes("UTF-8");
        }
    }

    private boolean _check_keycloak_auth_error(JsonObject result)
    {
        if (this.tsAuthUrl == null) return false;
        if (this.tsAuthClientId == null) return false;
        if (this.tsAuthClientSecret == null) return false;

        if (result == null) return false;
        JsonElement err = result.get("error");
        if (err == null) return false;
        JsonObject errObj = err.getAsJsonObject();
        if (errObj == null) return false;

        JsonElement code = errObj.get("code");
        if (code == null) return false;
        if (code.getAsInt() != 1001) return false;

        JsonElement msg = errObj.get("message");
        if (msg == null) return false;
        if (!msg.getAsString().equalsIgnoreCase("authorization error")) return false;

        try {
            reloadAccessToken();
            return true;
        } catch (MdtsdbException e) {
            return false;
        }
    }

    private JsonObject request(CommunicationLayer comLayer, JsonObject content) throws MdtsdbException
    {
        try {
            String url = getPath();
            boolean is_data_ep = url.endsWith("ingest");
            byte[] postData = _call_method_prepare_content(content, is_data_ep);
            return request0(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }
    }

    private JsonObject request0(CommunicationLayer comLayer, byte[] postData) throws MdtsdbException
    {
        JsonObject result = request0_impl(comLayer, postData);
        if (_check_keycloak_auth_error(result)) {
            result = request0_impl(comLayer, postData);
        }
        return result;
    }

    private JsonObject request0_impl(CommunicationLayer comLayer, byte[] postData) throws MdtsdbException
    {
        JsonObject result = null;

        try {
            String url = getPath();
            boolean is_data_ep = url.endsWith("ingest");
            Map<String, String> headers = _call_method_prepare_ep(is_data_ep);

            String data = comLayer.callApiMethod(headers, postData);
            result = new JsonParser().parse(data).getAsJsonObject();
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private JsonObject sendData(MdtsdbScheme schemeId, JsonElement sensorData) throws MdtsdbException
    {
        JsonObject result = null;

        try {
            String apiMethod = "setData";

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            String schemeIdString = schemeId.getSchemeId();
            postData.addProperty("context", schemeIdString);
            postData.addProperty("key", this.tsAppKey);
            postData.add("params", sensorData);

            if(this.tsAppKey == null || this.tsAppKey.isEmpty())
                postData.addProperty("adminkey", this.tsAdmKey);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod, schemeIdString);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private JsonObject sendGeoData(MdtsdbScheme schemeId, String geoData) throws MdtsdbException
    {
        JsonObject result = null;

        try {
            CommunicationLayer comLayer = getCommunicationLayer("setData", schemeId.getSchemeId());
            result = request0(comLayer, geoData.getBytes("UTF-8"));
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private JsonObject execQuery(MdtsdbScheme schemeId, String script, Integer version) throws MdtsdbException
    {
        return execQuery(schemeId, script, version, false);
    }

    private JsonObject execQuery(MdtsdbScheme schemeId, String script, Integer version, Boolean stream) throws MdtsdbException
    {
        JsonObject result = execQuery_impl(schemeId, script, version, stream);
        if (_check_keycloak_auth_error(result)) {
            result = execQuery_impl(schemeId, script, version, stream);
        }
        return result;
    }

    private JsonObject execQuery_impl(MdtsdbScheme schemeId, String script, Integer version, Boolean stream) throws MdtsdbException
    {
        JsonObject result = null;

        try {
            String q = String.format("q=%s&key=%s&adm=%s&stream=%d",
                URLEncoder.encode(script, "UTF-8"),
                URLEncoder.encode(this.tsAppKey, "UTF-8"),
                URLEncoder.encode(this.tsAdmKey, "UTF-8"),
                (stream ? 1 : 0)
            );

            String pref;
            if (schemeId == MdtsdbClientImpl.MdtsdbScheme.ASYNC_EVENTS)
            {
                schemeId = MdtsdbClientImpl.MdtsdbScheme.EVENTS;
                q += "&async=1";
            }

            CommunicationLayer comLayer = getCommunicationLayer(
                version == 1 ? MdtsdbClientImpl.QL : MdtsdbClientImpl.QL2, schemeId.getSchemeId());
            String data = comLayer.callApiMethod(q.getBytes("UTF-8"));
            result = new JsonParser().parse(data).getAsJsonObject();
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private String delayedQuery(String uuid, MdtsdbScheme schemeId) throws MdtsdbException
    {
        String result = delayedQuery_impl(uuid, schemeId);
        JsonObject resObj = null;
        try {
            resObj = new JsonParser().parse(result).getAsJsonObject();
        } catch(Exception e) {
            throw new MdtsdbException(e);
        }
        if (_check_keycloak_auth_error(resObj)) {
            result = delayedQuery_impl(uuid, schemeId);
        }
        return result;
    }

    private String delayedQuery_impl(String uuid, MdtsdbScheme schemeId) throws MdtsdbException
    {
        String result = null;
        String key = (this.tsAppKey != null && !this.tsAppKey.isEmpty()) ? this.tsAppKey : this.tsAdmKey;

        try {
            String q = String.format("uuid=%s&key=%s",
                URLEncoder.encode(uuid, "UTF-8"),
                URLEncoder.encode(key, "UTF-8"));

            CommunicationLayer comLayer = getCommunicationLayer(MdtsdbClientImpl.RESULTS, schemeId.getSchemeId());
            result = comLayer.callApiMethod(q.getBytes("UTF-8"));
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private JsonObject ping(MdtsdbScheme schemeId, Integer timeout) throws MdtsdbException
    {
        JsonObject paramsData = new JsonObject();
        paramsData.addProperty("timeout", timeout);
        return ping(schemeId, paramsData);
    }

    private JsonObject ping(MdtsdbScheme schemeId, JsonObject paramsData) throws MdtsdbException
    {
        JsonObject result = null;

        try {
            String apiMethod = "ping";

            JsonObject postData = new JsonObject();
            postData.addProperty("method", apiMethod);
            String schemeIdString = schemeId.getSchemeId();
            postData.addProperty("context", schemeIdString);
            postData.addProperty("key", this.tsAppKey);
            postData.add("params", paramsData);

            CommunicationLayer comLayer = getCommunicationLayer(apiMethod, schemeIdString);
            result = request(comLayer, postData);
        }
        catch(Exception e)
        {
            throw new MdtsdbException(e);
        }

        return result;
    }

    private CommunicationLayer getCommunicationLayer(String apiMethod) throws Exception
    {
        return getCommunicationLayer(apiMethod, "events");
    }

    private CommunicationLayer getCommunicationLayer(String apiMethod, String scheme) throws Exception
    {
        String contentType = "";
        String path = "";
        String signatureKey = "";
        if (CommonDataMethods.contains(apiMethod))
        {
            if(scheme.equals("kml"))
                path = "api/v1/ingest/kml";
            else if(scheme.equals("geo_events"))
                path = "api/v1/ingest/" + URLEncoder.encode(tsAppKey, "UTF-8");
            else
                path = "api/v1/ingest";
            contentType = "application/json";
            signatureKey = (tsAppKey != null && !tsAppKey.isEmpty()) ? tsAppKey : tsAdmKey;
        }
        else if (AdminMethods.contains(apiMethod))
        {
            path = "api/v1/admin";
            contentType = "application/json";
            signatureKey = tsAdmKey;
        }
        else if (apiMethod.equals(MdtsdbClientImpl.RESULTS))
        {
            path = "api/v1/result";
            contentType = "application/x-www-form-urlencoded";
            signatureKey = (tsAppKey != null && !tsAppKey.isEmpty()) ? tsAppKey : tsAdmKey;
            apiMethod = MdtsdbClientImpl.QL;
        }
        else if (apiMethod.equals(MdtsdbClientImpl.QL2))
        {
            path = "api/v1/ql?v=2";
            contentType = "application/x-www-form-urlencoded";
            signatureKey = (tsAppKey != null && !tsAppKey.isEmpty()) ? tsAppKey : tsAdmKey;
            apiMethod = MdtsdbClientImpl.QL;
        }
        else
        {
            path = "api/v1/ql";
            contentType = "application/x-www-form-urlencoded";
            signatureKey = (tsAppKey != null && !tsAppKey.isEmpty()) ? tsAppKey : tsAdmKey;
        }
        this.tsPath = path;
        return new CommunicationLayer(tsEndpoint, tsPort, getUseSSL(), isDebug, CommunicationLayer.HttpMethod.POST,
                                      contentType, path, tsSecretKey, signatureKey, apiMethod, scheme, this.tsAccessToken, this.tsAccessTokenType);
    }

    private String inputStreamGetData(InputStreamReader inputStreamReader) throws UnsupportedEncodingException, IOException {
        String txt = CharStreams.toString(inputStreamReader);
        if (txt.length() > 0 && txt.charAt(0) == '\u001e') {
            byte[] cbuf = txt.getBytes("US-ASCII");
            txt = new String(cbuf, 1, cbuf.length - 1);
        }
        return txt;
    }
}
