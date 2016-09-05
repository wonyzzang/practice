package idxcop;

import util.IndexSpecification;

public interface LeafFilterNode extends FilterNode {
	 FilterColumnValueDetail getFilterColumnValueDetail();
	
	 void setFilterColumnValueDetail(FilterColumnValueDetail
	 filterColumnValueDetail);
	
	 IndexSpecification getBestIndex();

}
