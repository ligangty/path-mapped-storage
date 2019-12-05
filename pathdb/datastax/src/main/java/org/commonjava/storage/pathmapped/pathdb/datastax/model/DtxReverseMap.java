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
import org.commonjava.storage.pathmapped.model.ReverseMap;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Table( name = "reversemap", readConsistency = "QUORUM", writeConsistency = "QUORUM" )
public class DtxReverseMap implements ReverseMap
{
    @PartitionKey
    private String fileId;

    @Column
    private HashSet<String> paths;

    public DtxReverseMap()
    {
    }

    public DtxReverseMap( String fileId, HashSet<String> paths )
    {
        this.fileId = fileId;
        this.paths = paths;
    }

    public String getFileId()
    {
        return fileId;
    }

    public void setFileId( String fileId )
    {
        this.fileId = fileId;
    }

    public Set<String> getPaths()
    {
        return paths;
    }

    public void setPaths( HashSet<String> paths )
    {
        this.paths = paths;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        DtxReverseMap that = (DtxReverseMap) o;
        return fileId.equals( that.fileId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fileId );
    }

    @Override
    public String toString()
    {
        return "DtxReverseMap{" + "fileId='" + fileId + '\'' + ", paths=" + paths + '}';
    }
}
