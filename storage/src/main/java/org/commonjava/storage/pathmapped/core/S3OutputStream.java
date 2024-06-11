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

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class S3OutputStream
                extends OutputStream
{

    /**
     * Default chunk size is 10MB
     */
    protected static final int BUFFER_SIZE = 10000000;

    /**
     * The path (key) name within the bucket
     */
    private final String path;

    /**
     * The metadata for the object
     */
    private final Map<String, String> metadata;

    /**
     * The temporary buffer used for storing the chunks
     */
    private final byte[] buf;

    private final S3Client s3Client;

    private final String bucket;

    /**
     * Collection of the etags for the parts that have been uploaded
     */
    private final List<String> etags;

    /**
     * The position in the buffer
     */
    private int position;

    /**
     * The unique id for this upload
     */
    private String uploadId;

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * Creates a new S3 OutputStream
     *
     * @param s3Client the AmazonS3 client
     * @param path     path within the bucket
     */
    public S3OutputStream( S3Client s3Client, String bucket, String path )
    {
        this(s3Client, bucket, path, null);
    }

    /**
     * Creates a new S3 OutputStream
     *
     * @param s3Client the AmazonS3 client
     * @param path     path within the bucket
     */
    public S3OutputStream( S3Client s3Client, String bucket, String path, Map<String, String> metadata )
    {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.path = path;
        this.metadata = metadata;
        buf = new byte[BUFFER_SIZE];
        position = 0;
        etags = new ArrayList<>();
        open = true;
    }

    public void cancel()
    {
        open = false;
        if ( uploadId != null )
        {
            s3Client.abortMultipartUpload( AbortMultipartUploadRequest.builder()
                                                                      .bucket( this.bucket )
                                                                      .key( path )
                                                                      .uploadId( uploadId )
                                                                      .build() );
        }
    }

    @Override
    public void write( int b )
    {
        assertOpen();
        if ( position >= buf.length )
        {
            flushBufferAndRewind();
        }
        buf[position++] = (byte) b;
    }

    /**
     * Write an array to the S3 output stream.
     *
     * @param b the byte-array to append
     */
    @Override
    public void write( byte[] b )
    {
        write( b, 0, b.length );
    }

    /**
     * Writes an array to the S3 Output Stream
     *
     * @param byteArray the array to write
     * @param o         the offset into the array
     * @param l         the number of bytes to write
     */
    @Override
    public void write( byte[] byteArray, int o, int l )
    {
        assertOpen();
        int ofs = o;
        int len = l;
        int size;
        while ( len > ( size = buf.length - position ) )
        {
            System.arraycopy( byteArray, ofs, buf, position, size );
            position += size;
            flushBufferAndRewind();
            ofs += size;
            len -= size;
        }
        System.arraycopy( byteArray, ofs, buf, position, len );
        position += len;
    }

    /**
     * Flushes the buffer by uploading a part to S3.
     */
    @Override
    public synchronized void flush()
    {
        assertOpen();
    }

    @Override
    public void close()
    {
        if ( open )
        {
            open = false;
            if ( uploadId != null )
            {
                if ( position > 0 )
                {
                    uploadPart();
                }

                CompletedPart[] completedParts = new CompletedPart[etags.size()];
                for ( int i = 0; i < etags.size(); i++ )
                {
                    completedParts[i] = CompletedPart.builder().eTag( etags.get( i ) ).partNumber( i + 1 ).build();
                }

                CompletedMultipartUpload completedMultipartUpload =
                                CompletedMultipartUpload.builder().parts( completedParts ).build();
                CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                                                                                                              .bucket(
                                                                                                                      bucket )
                                                                                                              .key( path )
                                                                                                              .uploadId(
                                                                                                                      uploadId )
                                                                                                              .multipartUpload(
                                                                                                                      completedMultipartUpload )
                                                                                                              .build();
                s3Client.completeMultipartUpload( completeMultipartUploadRequest );
            }
            else
            {
                PutObjectRequest putRequest = PutObjectRequest.builder()
                                                              .bucket( this.bucket )
                                                              .key( path )
                                                              .contentLength( (long) position )
                                                              .build();

                RequestBody requestBody =
                                RequestBody.fromInputStream( new ByteArrayInputStream( buf, 0, position ), position );
                s3Client.putObject( putRequest, requestBody );
            }
        }
    }

    private void assertOpen()
    {
        if ( !open )
        {
            throw new IllegalStateException( "Closed" );
        }
    }

    protected void flushBufferAndRewind()
    {
        if ( uploadId == null )
        {
            CreateMultipartUploadRequest.Builder uploadRequestBuilder =
                    CreateMultipartUploadRequest.builder().bucket( this.bucket ).key( path );
            if ( metadata != null && !metadata.isEmpty() )
            {
                uploadRequestBuilder.metadata( metadata );
            }
            CreateMultipartUploadResponse multipartUpload =
                    s3Client.createMultipartUpload( uploadRequestBuilder.build() );
            uploadId = multipartUpload.uploadId();
        }
        uploadPart();
        position = 0;
    }

    protected void uploadPart()
    {
        UploadPartRequest uploadRequest = UploadPartRequest.builder()
                                                           .bucket( bucket )
                                                           .key( path )
                                                           .uploadId( uploadId )
                                                           .partNumber( etags.size() + 1 )
                                                           .contentLength( (long) position )
                                                           .build();
        RequestBody requestBody = RequestBody.fromInputStream( new ByteArrayInputStream( buf, 0, position ), position );
        UploadPartResponse uploadPartResponse = s3Client.uploadPart( uploadRequest, requestBody );
        etags.add( uploadPartResponse.eTag() );
    }
}

