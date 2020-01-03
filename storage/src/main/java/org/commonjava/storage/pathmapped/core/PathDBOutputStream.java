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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import static java.util.Objects.isNull;

public class PathDBOutputStream
                extends FilterOutputStream
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

    private final ChecksumCalculator checksumCalculator;


    PathDBOutputStream( PathDB pathDB, PhysicalStore physicalStore, String fileSystem, String path, FileInfo fileInfo,
                        OutputStream out, String checksumAlgorithm )
            throws NoSuchAlgorithmException
    {
        super( out );
        this.pathDB = pathDB;
        this.physicalStore = physicalStore;
        this.fileSystem = fileSystem;
        this.path = path;
        this.fileInfo = fileInfo;
        this.fileId = fileInfo.getFileId();
        this.fileStorage = fileInfo.getFileStorage();
        this.checksumCalculator = new ChecksumCalculator( checksumAlgorithm );
    }

    @Override
    public void write( int b ) throws IOException
    {
        try
        {
            super.write( b );
            size += 1;
            byte by = (byte) ( b & 0xff );
            checksumCalculator.update( by );
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
    public void close() throws IOException
    {
        super.close();
        if ( isNull( error ) && size > 0 )
        {
            pathDB.insert( fileSystem, path, new Date(), fileId, size, fileStorage, checksumCalculator.getDigestHex() );
        }
    }
}
