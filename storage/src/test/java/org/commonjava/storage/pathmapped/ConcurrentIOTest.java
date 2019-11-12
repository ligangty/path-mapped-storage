/**
 * Copyright (C) 2013~2019 Red Hat, Inc.
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
package org.commonjava.storage.pathmapped;

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ConcurrentIOTest extends AbstractCassandraFMTest
{
    @Test
    public void concurrentRead()
            throws IOException, InterruptedException
    {
        File f = temp.newFile( "read-target.txt" );
        String src = "This is a test";
        try (OutputStream os = fileManager.openOutputStream( TEST_FS, f.getPath() ))
        {
            Assert.assertNotNull( os );
            IOUtils.write( src.getBytes(), os );
        }

        final int runs = 10;
        Executor executor = Executors.newFixedThreadPool( runs );
        final CountDownLatch latch = new CountDownLatch( runs );
        AtomicInteger failures = new AtomicInteger( 0 );
        for ( int i = 0; i < runs; i++ )
        {
            executor.execute( () -> {
                try (InputStream is = fileManager.openInputStream( TEST_FS, f.getPath() ))
                {
                    Assert.assertNotNull( is );
                    String result = new String( IOUtils.toByteArray( is ), Charset.defaultCharset() );
                    if ( !src.equals( result ) )
                    {
                        failures.addAndGet( 1 );
                    }
                }
                catch ( IOException e )
                {
                    failures.addAndGet( 1 );
                }
                finally
                {
                    latch.countDown();
                }
            } );
        }
        latch.await();
        Assert.assertThat( failures.get(), CoreMatchers.equalTo( 0 ) );
    }

    @Test
    @Ignore
    public void concurrentWrite()
    {
        //TODO: Cassandra based fm does not guarantee concurrent write on same file.
        //      Each single concurrent output will write its new file entry and the last successful
        //      one will be used in future in concurrent mode

        //TODO: question: so does this mean there will be some dirty-read here? like write-read-write case?
        Assert.fail( "concurrent write not support now!" );
    }
}
