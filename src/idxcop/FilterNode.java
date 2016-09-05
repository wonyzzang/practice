package idxcop;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;

import util.Column;
import util.IndexSpecification;

public interface FilterNode {
	 Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse();
	
	 Map<Column, List<Pair<IndexSpecification, Integer>>>
	 getPossibleUseIndices();
	
	 Map<Column, List<Pair<IndexSpecification, Integer>>>
	 getPossibleFutureUseIndices();
}
