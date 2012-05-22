package uk.bl.wap.util;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Maintains a BlockingQueue of Clamdscanner instances.
 * @author rcoram
 *
 */

public class ClamdScannerPool {
	private final LinkedBlockingDeque<ClamdScanner> objects;
	
	public ClamdScannerPool( String host, List<Integer> ports, int timeout ) {
		this.objects = new LinkedBlockingDeque<ClamdScanner>();
		this.add( host, ports, timeout );
	}

	public int add( String host, List<Integer> ports, int timeout ) {
		for( int port : ports ) {
			this.objects.push( new ClamdScanner( host, port, timeout ) );
		}
		return objects.size();
	}

	public int getPoolSize() {
		return this.objects.size();
	}

	public ClamdScanner borrow() {
		return this.objects.pop();
	}


	public void putBack( ClamdScanner scanner ) {
		this.objects.push( scanner );
	}
}
