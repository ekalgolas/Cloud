package commons.sample.log;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Yongtao on 9/13/2015.
 */
public class OutputStreamProxy extends FilterOutputStream {
	protected AtomicLong	counter;

	public OutputStreamProxy(final OutputStream out, final AtomicLong counter) {
		super(out);
		this.counter = counter;
	}

	@Override
	public void write(final int b) throws IOException {
		out.write(b);
		counter.incrementAndGet();
	}

	@Override
	public void write(final byte[] b) throws IOException {
		out.write(b);
		counter.addAndGet(b.length);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {
		out.write(b, off, len);
		counter.addAndGet(len);
	}
}
