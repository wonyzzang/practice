package mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class IndexCreationMapper extends TableMapper<ImmutableBytesWritable, Mutation> {
	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		resultToPut(context, row, value, conf);
	}

	private void resultToPut(Context context, ImmutableBytesWritable key, Result result, Configuration conf)
			throws IOException, InterruptedException {
		byte[] row = key.get();
		Put put = null;

		for (KeyValue kv : result.raw()) {
			if (kv.isDelete()) {
				// Skipping delete records as any way the deletes will mask the
				// actual
				// puts
				continue;
			} else {
				if (put == null) {
					put = new Put(row);
				}
				put.add(kv);
			}
		}
		if (put != null) {
			List<Put> indexPuts = IndexMapReduceUtil.getIndexPut(put, conf);
			for (Put idxPut : indexPuts) {
				context.write(new ImmutableBytesWritable(idxPut.getRow()), idxPut);
			}
		}
	}
}
