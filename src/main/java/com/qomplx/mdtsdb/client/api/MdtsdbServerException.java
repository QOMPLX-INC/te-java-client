/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.api;

public class MdtsdbServerException extends Exception
{
    static final public int ERR_CODE_AUTH_NOKEY = 1000;
    static final public int ERR_CODE_AUTH_PERM = 1001;
    static final public int ERR_CODE_AUTH_OVERLOAD = 1002;

    static final public int ERR_CODE_SERVICE_DENY = 2000;
    static final public int ERR_CODE_SERVICE_TIMEOUT = 2001;
    static final public int ERR_CODE_SERVICE_BADTIMEOUT = 2002;

    static final public int ERR_CODE_API_EXPECT = 3000;
    static final public int ERR_CODE_API_UNKNOWNMETH = 3001;

    static final public int ERR_CODE_QL_SYNTAX = 4000;
    static final public int ERR_CODE_QL_FORMAT = 4001;
    static final public int ERR_CODE_QL_NIFMATH = 4002;
    static final public int ERR_CODE_QL_UNKNOWN_METH = 4003;
    static final public int ERR_CODE_QL_GENERAL = 4004;
    static final public int ERR_CODE_QL_FILE_NOTFOUND = 4005;
    static final public int ERR_CODE_QL_UNKNOWN_SUBJ = 4006;
    static final public int ERR_CODE_QL_LOGICS = 4007;
    static final public int ERR_CODE_QL_EXPR = 4008;
    static final public int ERR_CODE_QL_TIMELESS = 4009;
    static final public int ERR_CODE_QL_SIZE = 4010;
    static final public int ERR_CODE_QL_MATHFAIL = 4011;

    static final public int ERR_CODE_REQ_NODATA = 5900;
    static final public int ERR_CODE_REQ_SIZE = 5000;
    static final public int ERR_CODE_REQ_LOADID = 5001;
    static final public int ERR_CODE_REQ_TIMEOUT = 5002;
    static final public int ERR_CODE_REQ_INTERNAL = 5003;
    static final public int ERR_CODE_REQ_S3ID = 5004;

    static final public int ERR_CODE_DB_INSUFF = 6000;
    static final public int ERR_CODE_DB_DISCON = 6001;
    static final public int ERR_CODE_DB_TIMEOUT = 6002;
    static final public int ERR_CODE_DB_INTERNAL = 6002;

    static final public int ERR_CODE_AMQP_GENERAL = 7000;
    static final public int ERR_CODE_AMQP_SEND = 7001;

    static final public int ERR_CODE_INCOME_FORMAT = 8000;

    static final public int ERR_CODE_GENERAL = 9000;
    static final public int ERR_CODE_INTERNAL = 9001;
    static final public int ERR_CODE_OFFLINE = 9002;

    final private int errorStatus;

    public MdtsdbServerException(int errorStatus, String errorMessage) {
        super(errorMessage);
        this.errorStatus = errorStatus;
    }

    public MdtsdbServerException(int errorStatus, String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.errorStatus = errorStatus;
    }

    public int getErrorStatus() {
        return this.errorStatus;
    }
}
