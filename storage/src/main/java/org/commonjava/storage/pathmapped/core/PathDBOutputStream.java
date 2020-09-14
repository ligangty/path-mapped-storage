/**
 * Copyright (C) 2019 Red Hat, Inc. (nos-devel@redhat.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.storage.pathmapped.core;

import org.commonjava.storage.pathmapped.spi.FileInfo;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.spi.PhysicalStore;
import org.commonjava.storage.pathmapped.util.ChecksumCalculator;

import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.isNull;
import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * PathDB output stream that saves a record to datastore only on successful completion.
 * This stream implementation *must* be wrapped using a BufferedOutputStream to be
 * efficient.
 */
public class PathDBOutputStream
                extends OutputStream
{
    private final PathDB pathDB;

    private final PhysicalStore physicalStore;

    private final String fileSystem;

    private final String path;

    private final FileInfo fileInfo;

    private final String fileId;

    private final String fileStorage;

    private long size;

    private Exception error;

    private ChecksumCalculator checksumCalculator;

    private final long timeoutInMilliseconds;

    private OutputStream out;

    private static final Logger logger = LoggerFactory.getLogger( PathDBOutputStream.class.getName() );

    private boolean isWarned = false;

    PathDBOutputStream( PathDB pathDB, PhysicalStore physicalStore, String fileSystem, String path, FileInfo fileInfo,
                        OutputStream out, String checksumAlgorithm, long timeoutInMilliseconds )
            throws NoSuchAlgorithmException
    {
        this.pathDB = pathDB;
        this.physicalStore = physicalStore;
        this.fileSystem = fileSystem;
        this.path = path;
        this.fileInfo = fileInfo;
        this.fileId = fileInfo.getFileId();
        this.fileStorage = fileInfo.getFileStorage();
        this.out = out;
        if ( isNotBlank( checksumAlgorithm ) && !checksumAlgorithm.equals( "NONE" ) )
        {
            this.checksumCalculator = new ChecksumCalculator( checksumAlgorithm );
        }
        this.timeoutInMilliseconds = timeoutInMilliseconds;
    }

    @Override
    public void write ( byte[] b, int off, int len )
            throws IOException
    {
        try
        {
            out.write( b, off, len );
            size += len;
            if ( checksumCalculator != null )
            {
                checksumCalculator.update( b, off, len );
            }
        }
        catch ( IOException e )
        {
            // the generated physical file should be deleted immediately
            physicalStore.delete( fileInfo );
            error = e;
            throw e;
        }
    }

    @Override
    /**
     *  Performance warning, this implementation is inefficient. Instead use
     *  write ( byte[] b, int off, int len ) by wrapping stream with a BufferedOutputStream.
     */
    public void write ( int b )  throws IOException
    {
        size += 1;
        byte by = (byte) ( b & 0xff );
        out.write ( by );
        if ( checksumCalculator != null )
        {
            checksumCalculator.update( by );
        }
        if ( !isWarned )
        {
            isWarned = true;
            logger.warn("Inefficient use of write( int ) with OutputStream.");
        }
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        out.close();
        if ( isNull( error ) )
        {
            Date creation = new Date();
            Date expiration = null;
            if ( timeoutInMilliseconds > 0 )
            {
                expiration = new Date( creation.getTime() + timeoutInMilliseconds );
            }
            String checksum = null;
            if ( checksumCalculator != null )
            {
                checksum = checksumCalculator.getDigestHex();
                logger.trace( "PathDBOutputStream: {} calculated checksum: {}", path, checksum );
            }
            pathDB.insert( fileSystem, path, creation, expiration, fileId, size, fileStorage, checksum );
        }
    }
}
