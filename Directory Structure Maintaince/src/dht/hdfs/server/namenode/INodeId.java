/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dht.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An id which uniquely identifies an inode. Id 1 to 1000 are reserved for
 * potential future usage. The id won't be recycled and is not expected to wrap
 * around in a very long time. Root inode id is always 1001. Id 0 is used for
 * backward compatibility support.
 */
public class INodeId {
	public static final long LAST_RESERVED_ID = 1000;
	public static final long ROOT_INODE_ID = LAST_RESERVED_ID + 1;
	public static final long GRANDFATHER_INODE_ID = 0;
	private final AtomicLong currentValue;

	INodeId() {
		currentValue = new AtomicLong(ROOT_INODE_ID);
	}

	public static void checkId(long requestId, INode inode)
			throws FileNotFoundException {
		if (requestId != GRANDFATHER_INODE_ID && requestId != inode.getId()) {
			throw new FileNotFoundException(
					"ID mismatch. Request id and saved id: " + requestId
							+ " , " + inode.getId());
		}
	}

	public long getCurrentValue() {
		return currentValue.get();
	}

	public void setCurrentValue(long value) {
		currentValue.set(value);
	}

	public long nextValue() {
		return currentValue.incrementAndGet();
	}

	public void skipTo(long newValue) throws IllegalStateException {
		for (;;) {
			final long c = getCurrentValue();
			if (newValue < c) {
				throw new IllegalStateException(
						"Cannot skip to less than the current value (=" + c
								+ "), where newValue=" + newValue);
			}

			if (currentValue.compareAndSet(c, newValue)) {
				return;
			}
		}
	}

	public boolean equals(final Object that) {
		if (that == null || this.getClass() != that.getClass()) {
			return false;
		}
		final AtomicLong thatValue = this.currentValue;
		return currentValue.equals(thatValue);
	}

	public int hashCode() {
		final long v = currentValue.get();
		return (int) v ^ (int) (v >>> 32);
	}
}
