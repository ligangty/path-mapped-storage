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

import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class GcTest
        extends AbstractCassandraFMTest
{
    @Test
    public void gcTest() throws Exception
    {
        writeWithContent( TEST_FS, path1, "simpleContent1" );
        writeWithContent( TEST_FS, path2, "simpleContent2" );

        PathDB pathDB = fileManager.getPathDB();
        List<Reclaim> orFiles = pathDB.listOrphanedFiles();
        assertEquals(0, orFiles.size());

        fileManager.delete(TEST_FS, path1);
        orFiles = pathDB.listOrphanedFiles();
        assertEquals(1, orFiles.size());

        fileManager.gc();

        orFiles = pathDB.listOrphanedFiles();
        assertEquals(0, orFiles.size());

        System.out.println("Test complete\n");
    }

}
