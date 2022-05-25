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
package org.commonjava.storage.pathmapped.pathdb.datastax.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.commonjava.storage.pathmapped.model.Filesystem;

import java.util.Objects;

@Table( name = "filesystem", readConsistency = "QUORUM", writeConsistency = "QUORUM" )
public class DtxFilesystem implements Filesystem
{
    @PartitionKey
    private String filesystem;

    @Column
    private long fileCount;

    @Column
    private long size;

    public DtxFilesystem()
    {
    }

    public DtxFilesystem(String filesystem, long fileCount, long size) {
        this.filesystem = filesystem;
        this.fileCount = fileCount;
        this.size = size;
    }

    @Override
    public String getFilesystem() {
        return filesystem;
    }

    @Override
    public Long getFileCount() {
        return fileCount;
    }

    @Override
    public Long getSize() {
        return size;
    }

    public void setFilesystem(String filesystem) {
        this.filesystem = filesystem;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DtxFilesystem that = (DtxFilesystem) o;
        return filesystem.equals(that.filesystem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesystem);
    }

    @Override
    public String toString() {
        return "DtxFilesystem{" +
                "filesystem='" + filesystem + '\'' +
                ", fileCount=" + fileCount +
                ", size=" + size +
                '}';
    }
}
