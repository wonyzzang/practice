package util;

public class ByteArrayBuilder {
	private short pos;
	private byte[] store;

	public ByteArrayBuilder(int size) {
		store = new byte[size];
		pos = 0;
	}

	public void put(byte[] src) {
		System.arraycopy(src, 0, store, pos, src.length);
		pos += src.length;
	}

	public void put(byte[] src, int offset, int length) {
		System.arraycopy(src, offset, store, pos, length);
		pos += length;
	}

	public static ByteArrayBuilder allocate(int size) {
		return new ByteArrayBuilder(size);
	}

	public short position() {
		return pos;
	}

	public void position(int newPosition) {
		pos = (short) newPosition;
	}

	public byte[] array() {
		return store;
	}

	public byte[] array(int offset, int length) {
		byte[] subArray = new byte[length];
		System.arraycopy(store, offset, subArray, 0, length);
		return subArray;
	}
}
