package org.commonjava.storage.pathmapped.spi;

import org.commonjava.storage.pathmapped.model.Filesystem;

public interface PathDBAdmin
{
    Filesystem getFilesystem(String filesystem);
}
