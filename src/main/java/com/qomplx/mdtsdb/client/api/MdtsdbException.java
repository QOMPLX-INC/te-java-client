/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.api;

public class MdtsdbException extends Exception
{
    public MdtsdbException(Exception e)
    {
        super(e);
    }

    public MdtsdbException(String errorMessage)
    {
        super(errorMessage);
    }
}
