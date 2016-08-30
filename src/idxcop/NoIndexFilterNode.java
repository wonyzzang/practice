package idxcop;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;

public class NoIndexFilterNode implements FilterNode {

	@Override
	public Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse() {
		return null;
	}

	@Override
	public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleUseIndices() {
		return null;
	}

	@Override
	public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleFutureUseIndices() {
		return null;
	}
}
