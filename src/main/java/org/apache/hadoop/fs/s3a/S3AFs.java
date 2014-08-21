package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

public class S3AFs extends DelegateToFileSystem {

    public S3AFs(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, new S3AFileSystem(), conf, "s3a", false);
    }

    @Override
    public int getUriDefaultPort() {
      return -1;
    }
    
}

