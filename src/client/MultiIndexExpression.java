package client;

import java.util.ArrayList;
import java.util.List;

public class MultiIndexExpression implements IndexExpression {
	private static final long serialVersionUID = 5322668904124942100L;

	private List<IndexExpression> indexExpressions = new ArrayList<IndexExpression>();

//	private GroupingCondition groupingCondition;
//
//	public MultiIndexExpression(GroupingCondition condition) {
//		this.groupingCondition = condition;
//	}
//
//	public GroupingCondition getGroupingCondition() {
//		return this.groupingCondition;
//	}
//
//	public void addIndexExpression(IndexExpression indexExpression) {
//		this.indexExpressions.add(indexExpression);
//	}

	public List<IndexExpression> getIndexExpressions() {
		return this.indexExpressions;
	}
}
