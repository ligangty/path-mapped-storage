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
package org.commonjava.storage.pathmapped.pathdb.jpa.model;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class JpaPathKey
                implements Serializable
{
    @Column( name = "filesystem", nullable = false )
    private String fileSystem; // correlates to the store key, e.g, 'maven:hosted:build_xyz'

    @Column( name = "parentpath", nullable = false )
    private String parentPath;

    @Column( nullable = false )
    private String filename;

    public JpaPathKey()
    {
    }

    public JpaPathKey( String fileSystem, String parentPath, String filename )
    {
        this.fileSystem = fileSystem;
        this.parentPath = parentPath;
        this.filename = filename;
    }

    public String getFileSystem()
    {
        return fileSystem;
    }

    public void setFileSystem( String fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public String getParentPath()
    {
        return parentPath;
    }

    public void setParentPath( String parentPath )
    {
        this.parentPath = parentPath;
    }

    public String getFilename()
    {
        return filename;
    }

    public void setFilename( String filename )
    {
        this.filename = filename;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        JpaPathKey pathKey = (JpaPathKey) o;
        return fileSystem.equals( pathKey.fileSystem ) && parentPath.equals( pathKey.parentPath ) && filename.equals(
                        pathKey.filename );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fileSystem, parentPath, filename );
    }

    @Override
    public String toString()
    {
        return "PathKey{" + "fileSystem='" + fileSystem + '\'' + ", parentPath='" + parentPath + '\'' + ", filename='"
                        + filename + '\'' + '}';
    }

}
