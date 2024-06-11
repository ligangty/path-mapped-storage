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
import org.commonjava.storage.pathmapped.spi.PhysicalStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import static java.lang.System.currentTimeMillis;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.getRandomFileId;

public class S3PhysicalStore implements PhysicalStore
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final S3Client s3Client;
    private final String bucket;

    public S3PhysicalStore( S3Client s3Client, String bucket )
    {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public FileInfo getFileInfo( String fileSystem, String path )
    {
        String id = getRandomFileId();
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFileId( id );
        fileInfo.setFileStorage( getS3Key( fileSystem, path ) );
        return fileInfo;
    }

    /**
     * Some characters that might require special handling, like Colon ':'. This default impl replaces colon
     * with slash '/' and appends a timestamp to path to avoid conflict. (e.g, if a file is deleted and put again,
     * the new physical file can be removed by GC after the grace period if the 'storagefile' are same).
     * The timestamp uses the last 5 digital of current-time-millis.
     * @return valid S3 key string
     */
    protected String getS3Key( String filesystem, String path )
    {
        String tmp = Long.toString( currentTimeMillis() );
        String timestamp = tmp.substring( tmp.length() - 5 );
        return Paths.get( filesystem.replaceAll(":", "/"), path + "." + timestamp ).toString();
    }

    @Override
    public OutputStream getOutputStream( FileInfo fileInfo )
    {
        try
        {
            return new S3OutputStream( this.s3Client, this.bucket, fileInfo.getFileStorage() );
        }
        catch ( S3Exception e )
        {
            logger.debug( "Cannot create file: {}, got error: {}", fileInfo.getFileStorage(), e.toString() );
            return null;
        }
    }

    @Override
    public InputStream getInputStream( String storageFile )
    {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                            .bucket( this.bucket )
                                                            .key( storageFile )
                                                            .build();

        try
        {
            return this.s3Client.getObject( getObjectRequest );
        }
        catch ( S3Exception e )
        {
            logger.debug( "Target file not exists, file: {}, got error: {}", storageFile, e.toString() );
            return null;
        }
    }

    @Override
    public boolean delete( FileInfo fileInfo )
    {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                                                                     .bucket( this.bucket )
                                                                     .key( fileInfo.getFileStorage() )
                                                                     .build();
        try
        {
            this.s3Client.deleteObject( deleteObjectRequest );
            return true;
        }
        catch ( S3Exception e )
        {
            logger.error( "Failed to delete file: " + fileInfo, e );
            return false;
        }
    }

    @Override
    public boolean exists( String storageFile )
    {
        if ( storageFile == null )
        {
            return false;
        }
        try
        {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                                                                   .bucket( this.bucket )
                                                                   .key( storageFile )
                                                                   .build();
            this.s3Client.headObject( headObjectRequest );
            return true;
        }
        catch ( S3Exception e )
        {
            if ( e.statusCode() == 404 )
            {
                return false;
            }
            else
            {
                throw e;
            }
        }
    }
}
