package util;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Constants {

  public static final int DEFAULT_NUM_RETRIES = 10;

  public static final long DEFAULT_PAUSE = 1000;

  public static final int DEFAULT_RETRY_LONGER_MULTIPLIER = 10;

  public static final byte[] IDX_COL_FAMILY = Bytes.toBytes("d");

  public static final byte[] IDX_COL_QUAL = new byte[0];

  public static final String INDEX_TABLE_SUFFIX = "_idx";

  public static final int DEF_MAX_INDEX_NAME_LENGTH = 18;

  public static final String INDEX_EXPRESSION = "indexExpression";

}
