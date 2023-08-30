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

import org.commonjava.storage.pathmapped.spi.PathDB;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

public class CopyAndExpireTest
        extends AbstractCassandraFMTest
{
    /**
     * This case uses two files to test copy-and-expire and copy-with-expiration api
     */
    @Test
    public void copyAndExpire() throws Exception
    {
        final long timeoutSeconds = 1;

        final String source = "sourceFS";
        final String target = "targetFS";

        // Prepare two files in the source
        writeWithContent( source, path1, "simpleContent1", timeoutSeconds, TimeUnit.SECONDS );
        writeWithContent( source, path2, "simpleContent2", timeoutSeconds, TimeUnit.SECONDS );

        // Copy path1 and call expire() to make the target file never expires
        fileManager.copy( source, path1, target, path1);
        fileManager.expire( target, path1, null );

        // Copy path2 with specified creation and expiration null
        Date creation = new Date();
        fileManager.copy( source, path2, target, path2, creation, null );

        PathDB pathDB = fileManager.getPathDB();

        // Check 1. Both path1 and path2 are there before timeout
        String storageFile = pathDB.getStorageFile(source, path1);
        //System.out.println(">>>" + storageFile);
        assertNotNull(storageFile);

        storageFile = pathDB.getStorageFile(source, path2);
        //System.out.println(">>>" + storageFile);
        assertNotNull(storageFile);

        // Sleep...
        sleep(timeoutSeconds * 1000);

        // Check 2. Both path1 and path2 in source are expired
        storageFile = pathDB.getStorageFile(source, path1);
        //System.out.println(">>>" + storageFile);
        assertNull(storageFile);

        storageFile = pathDB.getStorageFile(source, path2);
        //System.out.println(">>>" + storageFile);
        assertNull(storageFile);

        // Check 3. Both path1 and path2 in target are not expired
        storageFile = pathDB.getStorageFile(target, path1);
        //System.out.println(">>>" + storageFile);
        assertNotNull(storageFile);

        storageFile = pathDB.getStorageFile(target, path2);
        //System.out.println(">>>" + storageFile);
        assertNotNull(storageFile);
    }

}
