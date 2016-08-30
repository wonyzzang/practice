package client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class IndexUtils {
	private IndexUtils() {
		// Avoid instantiation of this class.
	}

	public static byte[] toBytes(IndexExpression indexExpression) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(indexExpression);
		return bos.toByteArray();
	}

	public static IndexExpression toIndexExpression(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = new ObjectInputStream(bis);
		return (IndexExpression) ois.readObject();
	}

}
