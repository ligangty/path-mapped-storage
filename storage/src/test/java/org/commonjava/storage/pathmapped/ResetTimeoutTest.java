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
package org.commonjava.storage.pathmapped;

import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

public class ResetTimeoutTest
        extends AbstractCassandraFMTest
{
    /**
     * This case tests reset file timeout for accessing.
     */
    @Test
    public void run() throws Exception
    {
        final long timeoutSeconds = 1;
        final String content = "This is a test";

        // Prepare the test file which will expire in '1s'
        writeWithContent( TEST_FS, path1, content, timeoutSeconds, TimeUnit.SECONDS );

        PathDB pathDB = fileManager.getPathDB();

        // Check the original expiration
        PathMap pathMap = fileManager.getPathMap(TEST_FS, path1);
        System.out.println(">>> (original) " + pathMap.getExpiration());

        // File is NOT timeout at this moment
        String storageFile = pathDB.getStorageFile(TEST_FS, path1);
        //System.out.println(">>>" + storageFile);
        assertNotNull(storageFile);

        // Access the file, extend the expiration (default 12h)
        checkRead(TEST_FS, path1, true, content);

        // Sleep...
        sleep(timeoutSeconds * 1000);

        // Check file is NOT expired
        pathMap = fileManager.getPathMap(TEST_FS, path1);
        Date newTimeout = pathMap.getExpiration();
        System.out.println(">>> (extended) " + newTimeout);
        storageFile = pathDB.getStorageFile(TEST_FS, path1);
        //System.out.println(">>>" + storageFile);
        assertNotNull(storageFile);

        // File accessed again, extend the expiration one more time (after sleep, the timeout is less than 12h from now)
        checkRead(TEST_FS, path1, true, content);
        pathMap = fileManager.getPathMap(TEST_FS, path1);
        Date finalTimeout = pathMap.getExpiration();
        System.out.println(">>> (final) " + finalTimeout);

        // Future file accessing, expiration not change
        checkRead(TEST_FS, path1, true, content);
        pathMap = fileManager.getPathMap(TEST_FS, path1);
        assertEquals( finalTimeout, pathMap.getExpiration() );
    }

}
