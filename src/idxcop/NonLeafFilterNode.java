package idxcop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;

public class NonLeafFilterNode implements FilterNode {

	private List<FilterNode> filterNodes = new ArrayList<FilterNode>();

	private GroupingCondition groupingCondition;

	private Map<List<FilterColumnValueDetail>, IndexSpecification> indicesToUse = new HashMap<List<FilterColumnValueDetail>, IndexSpecification>();

	public NonLeafFilterNode(GroupingCondition condition) {
		this.groupingCondition = condition;
	}

	public GroupingCondition getGroupingCondition() {
		return groupingCondition;
	}

	public List<FilterNode> getFilterNodes() {
		return filterNodes;
	}

	public void addFilterNode(FilterNode filterNode) {
		this.filterNodes.add(filterNode);
	}

	public void addIndicesToUse(FilterColumnValueDetail f, IndexSpecification i) {
		List<FilterColumnValueDetail> key = new ArrayList<FilterColumnValueDetail>(1);
		key.add(f);
		this.indicesToUse.put(key, i);
	}

	public void addIndicesToUse(List<FilterColumnValueDetail> lf, IndexSpecification i) {
		this.indicesToUse.put(lf, i);
	}

	@Override
	public Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse() {
		return this.indicesToUse;
	}

	@Override
	public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleUseIndices() {
		return null;
	}

	@Override
	public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleFutureUseIndices() {
		// There is no question of future use possible indices on a non leaf
		// node.
		return null;
	}
}
