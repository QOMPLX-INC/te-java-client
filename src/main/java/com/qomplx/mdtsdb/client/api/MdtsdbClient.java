/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.api;

import java.util.*;

import com.qomplx.mdtsdb.client.impl.MdtsdbClientImpl;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;

public class MdtsdbClient
{
    /**
     * Creates a Mdtsdb client with a custom end-point.
     *
     */
    public MdtsdbClient(String tsEndpoint, int tsPort, String tsAppKey, String tsAdmKey, String tsSecretKey, Properties options)
    {
        this.tsClient = new MdtsdbClientImpl(tsEndpoint, tsPort, tsAppKey, tsAdmKey, tsSecretKey, options);
    }

    /**
     * Creates a Mdtsdb client with a standard end-point.
     *
     */
    public MdtsdbClient(String tsAppKey, String tsAdmKey, String tsSecretKey, Properties options)
    {
        this.tsClient = new MdtsdbClientImpl(tsAppKey, tsAdmKey, tsSecretKey, options);
    }

    /**
     * Creates a Mdtsdb client using new client implementation.
     *
     */
    private MdtsdbClient(MdtsdbClientImpl newClient)
    {
        this.tsClient = newClient;
    }


    /**
     * Set keycloak access token credentials
     */
    public void setAccessCredentials(String authUrl, String clientId, String clientSecret) throws MdtsdbException
    {
        this.tsClient.setAccessCredentials(authUrl, clientId, clientSecret);
    }

    /**
     * Set keycloak access token
     */
    public void setAccessToken(String accessToken) throws MdtsdbException
    {
        this.tsClient.setAccessToken(accessToken);
    }

    /**
     * Set keycloak access token
     */
    public void setAccessToken(String accessToken, String accessTokenType) throws MdtsdbException
    {
        this.tsClient.setAccessToken(accessToken, accessTokenType);
    }

    /**
     * @return the access token
     */
    public String getAccessToken()
    {
        return this.tsClient.getAccessToken();
    }

    /**
     * @return the access token type
     */
    public String getAccessTokenType()
    {
        return this.tsClient.getAccessTokenType();
    }

    /**
     * Enables Debug output.
     *
     */
    public void enableDebugOutput()
    {
        this.tsClient.enableDebugOutput();
    }

    /**
     * @return the administrative key
     */
    public String getAdmKey()
    {
        return this.tsClient.getAdmKey();
    }

    /**
     * @return the application key
     */
    public String getAppKey()
    {
        return this.tsClient.getAppKey();
    }

    /**
     * @return the secret key
     */
    public String getSecretKey()
    {
        return this.tsClient.getSecretKey();
    }

    //////////////
    // Factory API

    /**
     * Creates a Mdtsdb client that solves administrative tasks.
     *
     * @param tsAdmKey administrative key
     * @param tsSecretKey security key
     */
    public MdtsdbClient newAdmClient(String tsAdmKey, String tsSecretKey)
    {
        return new MdtsdbClient(this.tsClient.newAdmClient(tsAdmKey, tsSecretKey));
    }

    /**
     * Creates a Mdtsdb client to create/query data.
     *
     * @param tsAppKey application (swimlane) key
     * @param tsSecretKey security key
     */
    public MdtsdbClient newClient(String tsAppKey, String tsSecretKey)
    {
        return new MdtsdbClient(this.tsClient.newClient(tsAppKey, tsSecretKey));
    }

    ///////////
    // Data API

    /**
     * @param sensorData json object, mapping a sensor identifier to the sensor value
     *
     * @see MdtsdbClientImpl#sendEventsData(JsonObject)
     */

    public JsonObject sendEventsData(JsonObject sensorData) throws MdtsdbException
    {
        return this.tsClient.sendEventsData(sensorData);
    }

    /**
     * @param sensorData json object, mapping a sensor identifier to the sensor value
     *
     * @see MdtsdbClientImpl#sendEventsData(JsonArray)
     */

    public JsonObject sendEventsData(JsonArray sensorData) throws MdtsdbException
    {
        return this.tsClient.sendEventsData(sensorData);
    }

    /**
     * @param sensorData json object, mapping a sensor identifier to the sensor value
     *
     * @see MdtsdbClientImpl#insert(JsonArray)
     */

    public JsonObject insert(JsonArray sensorData) throws MdtsdbException
    {
        return this.tsClient.sendEventsData(sensorData);
    }

    /**
     * @param GeojsonOrKml string in either GeoJSON, TopoJSON or KML format
     *
     * @see MdtsdbClientImpl#sendEventsGeoData(String)
     */

    public JsonObject sendEventsGeoData(String GeojsonOrKml) throws MdtsdbException
    {
        return this.tsClient.sendEventsGeoData(GeojsonOrKml);
    }

    /**
     * @param kmlContent sensor data in Keyhole Markup Language format
     * @param defaultParams maps sensor properties to default values
     *
     * @see MdtsdbClientImpl#uploadKml(String, Properties)
     */

    public JsonObject uploadKml(String kmlContent, Properties defaultParams) throws MdtsdbException
    {
        return this.tsClient.uploadKml(kmlContent, defaultParams);
    }

    /**
     * @param filePath a path to the file with sensor data in Keyhole Markup Language format
     * @param defaultParams maps sensor properties to default values
     *
     * @see MdtsdbClientImpl#uploadKmlFile(String, Properties)
     */

    public JsonObject uploadKmlFile(String filePath, Properties defaultParams) throws MdtsdbException
    {
        return this.tsClient.uploadKmlFile(filePath, defaultParams);
    }

    /**
     * Ping the service.
     *
     * @param timeout either maximum number of milliseconds to wait, or null for infinity
     *
     * @see MdtsdbClientImpl#ping(Integer)
     */

    public JsonObject ping(Integer timeout) throws MdtsdbException
    {
        return this.tsClient.ping(timeout);
    }

    ////////////
    // Query API

    /**
     * @param script query language script content
     *
     * @see MdtsdbClientImpl#eventsQuery(String)
     */

    public JsonObject eventsQuery(String script) throws MdtsdbException
    {
        return this.tsClient.eventsQuery(script, 1);
    }

    public JsonObject eventsQuery(String script, Boolean streamBody) throws MdtsdbException
    {
        return this.tsClient.eventsQuery(script, 1, streamBody);
    }

    /**
     * @param script query language script content
     *
     * @see MdtsdbClientImpl#query(String)
     */

    public JsonObject query(String script) throws MdtsdbException
    {
        return this.tsClient.query(script);
    }

    public JsonObject query(String script, Boolean streamBody) throws MdtsdbException
    {
        return this.tsClient.query(script, streamBody);
    }

    /**
     * @param script query language script content
     *
     * @see MdtsdbClientImpl#asyncEventsQuery(String)
     */

    public JsonObject asyncEventsQuery(String script) throws MdtsdbException
    {
        return this.tsClient.asyncEventsQuery(script, 1);
    }

    public JsonObject asyncEventsQuery(String script, Boolean streamBody) throws MdtsdbException
    {
        return this.tsClient.asyncEventsQuery(script, 1, streamBody);
    }

    /**
     * @param script query language script content
     *
     * @see MdtsdbClientImpl#asyncQuery(String)
     */

    public JsonObject asyncQuery(String script) throws MdtsdbException
    {
        return this.tsClient.asyncQuery(script);
    }

    public JsonObject asyncQuery(String script, Boolean streamBody) throws MdtsdbException
    {
        return this.tsClient.asyncQuery(script, streamBody);
    }

    /**
     * @param uuid identifier of the stored data, as returned in details of
     *             the response with notification about delayed execution
     *
     * @see MdtsdbClientImpl#getStored(String)
     */

    public String getStored(String uuid) throws MdtsdbException
    {
        return this.tsClient.getStored(uuid);
    }

    /**
     * @see MdtsdbClientImpl#getEventsMessages()
     */

    public JsonObject getMessages() throws MdtsdbException
    {
        return this.tsClient.getMessages();
    }

    ////////////
    // Admin API

    /**
     * Creates a new application key. Requires an admin key.
     *
     * @param comment details of the created user of the application key
     * @param suggestName suggested application key (user is able to select application key if it does not exist)
     *
     * @see MdtsdbClientImpl#newAppkey(String, String)
     */

    public JsonObject newAppkey(String comment, String suggestName) throws MdtsdbException
    {
        return this.tsClient.newAppkey(comment, suggestName);
    }

    /**
     * Creates a new application key. Requires an admin key.
     *
     * @param comment details of the created user of the application key
     * @see MdtsdbClientImpl#newAppkey(String)
     */

    public JsonObject newAppkey(String comment) throws MdtsdbException
    {
        return this.tsClient.newAppkey(comment);
    }

    /**
     * Creates a new application key. Requires an admin key.
     *
     * @param comment     details of the created user of the application key
     * @param optsData        swimlane options
     *
     * @see MdtsdbClientImpl#newAppkey(String, JsonObject)
     */

    public JsonObject newAppkey(String comment, JsonObject optsData) throws MdtsdbException
    {
        return this.tsClient.newAppkey(comment, optsData);
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
        return this.tsClient.getOrCreateAppkey(comment, suggestName);
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
        return this.tsClient.getOrCreateAppkey(comment, optsData);
    }

    /**
     * Deletes the application key. Requires an admin key.
     *
     * <p>
     *   The administrator key must be the same key that was used to create the application key.
     * </p>
     *
     * @param appKey application key to delete
     * @see MdtsdbClientImpl#deleteAppkey(String)
     */

    public JsonObject deleteAppkey(String appKey) throws MdtsdbException
    {
        return this.tsClient.deleteAppkey(appKey);
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
     * @see MdtsdbClientImpl#deleteAppkey(String, Boolean)
     */

    public JsonObject deleteAppkey(String appKey, Boolean keepData) throws MdtsdbException
    {
        return this.tsClient.deleteAppkey(appKey, keepData);
    }

    /**
     * Creates a new admin key. Requires an admin key with super-user rights.
     *
     * @param comment details of the created user of the application key
     * @see MdtsdbClientImpl#newAdminkey(String)
     */

    public JsonObject newAdminkey(String comment) throws MdtsdbException
    {
        return this.tsClient.newAdminkey(comment);
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
        return this.tsClient.getOrCreateAdminkey(admKey, comment);
    }

    /**
     * Deletes the admin key. Requires an admin key with super-user rights.
     *
     * @param admKey administrative key to delete
     * @see MdtsdbClientImpl#deleteAdminkey(String)
     */

    public JsonObject deleteAdminkey(String admKey) throws MdtsdbException
    {
        return this.tsClient.deleteAdminkey(admKey);
    }

    ///////////////
    // Ws API

    /**
     * Build a target URL for websocket connection.
     *
     */
    public String wsTargetUrl() throws Exception
    {
        return this.tsClient.wsTargetUrl();
    }

    /**
     * Build a target URL for websocket connection.
     *
     * @param async null if async mode is not predefined, true for async mode, false otherwise
     * @param defAppKey set not null if the predefined application key must be set in administrative websocket connection
     */
    public String wsTargetUrl(Boolean async, String defAppKey) throws Exception
    {
        return this.tsClient.wsTargetUrl(async, defAppKey);
    }

    /**
     * Build the authorization header for websocket connection.
     *
     */
    public String wsAuthorizationHeader() throws Exception
    {
        return this.tsClient.wsAuthorizationHeader();
    }

    /**
     * Build the authorization header for websocket connection.
     *
     * @param async null if async mode is not predefined, true for async mode, false otherwise
     * @param defAppKey set not null if the predefined application key must be set in administrative websocket connection
     */

    public String wsAuthorizationHeader(Boolean async, String defAppKey) throws Exception
    {
        return this.tsClient.wsAuthorizationHeader(async, defAppKey);
    }

    /**
     * Build a query frame to send using websocket connection. Events mode.
     *
     * @param query text of the query to execute
     * @param async null if async mode is not predefined, set true for async mode, false otherwise
     * @see MdtsdbClientImpl#wsBuildQuery(String, Boolean)
     */

    public String wsBuildQuery(String query, Boolean async) throws Exception
    {
        return this.tsClient.wsBuildQuery(query, async, false);
    }

    /**
     * Build a query frame to send using websocket connection. Events mode.
     *
     * @param query text of the query to execute
     * @param async null if async mode is not predefined, set true for async mode, false otherwise
     * @param streamBody set true for stream body data, false otherwise
     * @see MdtsdbClientImpl#wsBuildQuery(String, Boolean, Boolean)
     */

    public String wsBuildQuery(String query, Boolean async, Boolean streamBody) throws Exception
    {
        return this.tsClient.wsBuildQuery(query, async, streamBody);
    }

    /**
     * Build a query frame to uploads data from sensors to server using websocket connection.
     *
     * <p>
     *   See sendStreamingData() for the detailed description of the method parameter.
     * </p>
     *
     * @param sensorData json object or array, mapping a sensor identifier to the sensor value
     * @see MdtsdbClientImpl#wsBuildSendData(JsonElement, Boolean)
     */

    public String wsBuildSendData(JsonElement sensorData, Boolean async) throws Exception
    {
        return this.tsClient.wsBuildSendData(sensorData, async);
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
        return this.tsClient.wsBuildUploadKml(kmlContent, defaultParams, async);
    }

    /**
     * @param timeout either maximum number of milliseconds to wait, or null for infinity
     * @param async set true for async mode, false otherwise
     * @see MdtsdbClientImpl#wsBuildPing(Integer, Boolean)
     */

    public String wsBuildPing(Integer timeout, Boolean async) throws Exception
    {
        return this.tsClient.wsBuildPing(timeout, async);
    }

    /**
     *  Build a query frame to query data, which were stored after delayed execution of the query.
     *
     * @param uuid identifier of the stored data, as returned in details of
     *             the response with notification about delayed execution
     * @param async set true for async mode, false otherwise
     * @see MdtsdbClientImpl#wsBuildGetStored(String, Boolean)
     */

    public String wsBuildGetStored(String uuid, Boolean async) throws Exception
    {
        return this.tsClient.wsBuildGetStored(uuid, async);
    }

    /**
     * Build a query notification messages.
     *
     * @param async set true for async mode, false otherwise
     * @see MdtsdbClientImpl#wsBuildGetMessages(Boolean)
     */

    public String wsBuildGetMessages(Boolean async) throws Exception
    {
        return this.tsClient.wsBuildGetMessages(async);
    }


    private MdtsdbClientImpl tsClient = null;
}
