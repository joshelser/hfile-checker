package com.github.joshelser.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 */
public class HFileChecker extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(HFileChecker.class);

  @Override
  public int run(String[] args) throws Exception {
    if (getConf() == null) {
      throw new RuntimeException("A Configuration instance must be provided.");
    }
    FSUtils.setFsDefault(getConf(), FSUtils.getRootDir(getConf()));

    // iterate over all files found
    for (String fileName : args) {
      try {
        processFile(new Path(fileName));
      } catch (Exception e) {
        LOG.error("Error reading " + fileName, e);
        System.exit(-2);
      }
    }

    return 0;
  }

  private void processFile(Path file) throws Exception {
    FileSystem fs = file.getFileSystem(getConf());
    if (!fs.exists(file)) {
      System.err.println("ERROR, file doesnt exist: " + file);
      throw new IOException(file.toString() + " does not exist");
    }

    HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(getConf()), getConf());

    System.out.println("Block Headers:");
    /*
     * TODO: this same/similar block iteration logic is used in HFileBlock#blockRange and
     * TestLazyDataBlockDecompression. Refactor?
     */
    FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, file);

    // Make sure we use the HBase Checksum
    Field useHBaseXsumField = FSDataInputStreamWrapper.class.getDeclaredField("useHBaseChecksum");
    useHBaseXsumField.setAccessible(true);
    useHBaseXsumField.set(fsdis, true);
    
    long fileSize = fs.getFileStatus(file).getLen();
    FixedFileTrailer trailer =
      FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
    long offset = trailer.getFirstDataBlockOffset(),
      max = trailer.getLastDataBlockOffset();
    HFileBlock block;
    Method toStringHeaderMethod = HFileBlock.class.getDeclaredMethod("toStringHeader", ByteBuffer.class);
    toStringHeaderMethod.setAccessible(true);

    while (offset <= max) {
      block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
        /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);

      System.out.println(toStringHeaderMethod.invoke(null, block.getBufferReadOnly()));

      offset += block.getOnDiskSizeWithHeader();
      System.out.println(block + "\n");
    }

    reader.close();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    // no need for a block cache
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    int ret = ToolRunner.run(conf, new HFileChecker(), args);
    System.exit(ret);
  }
}
