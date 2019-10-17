package org.commonjava.storage.pathmapped.model;

import java.util.Date;

public interface PathMap
{
    String getFileSystem();

    String getParentPath();

    String getFilename();

    String getFileId();

    int getSize();

    String getFileStorage();

    Date getCreation();
}
