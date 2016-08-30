package client;

import java.util.ArrayList;
import java.util.List;

public class SingleIndexExpression implements IndexExpression {
	private static final long serialVersionUID = 893160134306193043L;

	private String indexName;

	private List<EqualsExpression> equalsExpressions = new ArrayList<EqualsExpression>();

	private RangeExpression rangeExpression;

	public SingleIndexExpression(String indexName) {
		this.indexName = indexName;
	}

	public String getIndexName() {
		return indexName;
	}

	/**
	 * This is expected to be called in the order of columns specified in the
	 * Index. If index is on columns cf1:c1, cf1:c2 and cf2:c3 when creating the
	 * SingleIndexExpression call this method in the same order as of above
	 * 
	 * @param equalsExpression
	 */
	public void addEqualsExpression(EqualsExpression equalsExpression) {
		this.equalsExpressions.add(equalsExpression);
	}

	public List<EqualsExpression> getEqualsExpressions() {
		return this.equalsExpressions;
	}

	public void setRangeExpression(RangeExpression rangeExpression) {
		this.rangeExpression = rangeExpression;
	}

	public RangeExpression getRangeExpression() {
		return this.rangeExpression;
	}
}
