package idxcop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;

public class PossibleIndexFilterNode implements LeafFilterNode {

	private List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices;

	private FilterColumnValueDetail filterColumnValueDetail;

	public PossibleIndexFilterNode(List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices,
			FilterColumnValueDetail filterColumnValueDetail) {
		this.possibleFutureUseIndices = possibleFutureUseIndices;
		this.filterColumnValueDetail = filterColumnValueDetail;
	}

	@Override
	public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleFutureUseIndices() {
		// TODO avoid create of Map instance all the time...
		Map<Column, List<Pair<IndexSpecification, Integer>>> reply = new HashMap<Column, List<Pair<IndexSpecification, Integer>>>();
		reply.put(filterColumnValueDetail.getColumn(), possibleFutureUseIndices);
		return reply;
	}

	@Override
	public Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse() {
		return null;
	}

	public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleUseIndices() {
		return null;
	}

	@Override
	public FilterColumnValueDetail getFilterColumnValueDetail() {
		return this.filterColumnValueDetail;
	}

	@Override
	public void setFilterColumnValueDetail(FilterColumnValueDetail filterColumnValueDetail) {
		this.filterColumnValueDetail = filterColumnValueDetail;
	}

	@Override
	public IndexSpecification getBestIndex() {
		return null;
	}
}
