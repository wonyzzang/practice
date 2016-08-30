package client;

public class RangeExpression implements IndexExpression {
	private static final long serialVersionUID = 8772267632040419734L;

//	private Column column;
//	private byte[] lowerBoundValue;
//	private byte[] upperBoundValue;
//	private boolean lowerBoundInclusive;
//	private boolean upperBoundInclusive;
//
//	public Column getColumn() {
//		return column;
//	}

//	public byte[] getLowerBoundValue() {
//		return lowerBoundValue;
//	}
//
//	public byte[] getUpperBoundValue() {
//		return upperBoundValue;
//	}
//
//	public boolean isLowerBoundInclusive() {
//		return lowerBoundInclusive;
//	}
//
//	public boolean isUpperBoundInclusive() {
//		return upperBoundInclusive;
//	}
//
//	/**
//	 * When the range is non closed at one end (to specific upper bound but only
//	 * lower bound) pass the corresponding bound value as null.
//	 * 
//	 * @param column
//	 * @param lowerBoundValue
//	 * @param upperBoundValue
//	 * @param lowerBoundInclusive
//	 * @param upperBoundInclusive
//	 */
//	public RangeExpression(Column column, byte[] lowerBoundValue, byte[] upperBoundValue, boolean lowerBoundInclusive,
//			boolean upperBoundInclusive) {
//		if (column == null || (lowerBoundValue == null && upperBoundValue == null)) {
//			throw new IllegalArgumentException();
//		}
//		this.column = column;
//		this.lowerBoundValue = lowerBoundValue;
//		this.upperBoundValue = upperBoundValue;
//		this.lowerBoundInclusive = lowerBoundInclusive;
//		this.upperBoundInclusive = upperBoundInclusive;
//	}
//
//	@Override
//	public String toString() {
//		return "RangeExpression : Column[" + this.column + "], lowerBoundInclusive : " + this.lowerBoundInclusive
//				+ ", upperBoundInclusive : " + this.upperBoundInclusive;
//	}
}
