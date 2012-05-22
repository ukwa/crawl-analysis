package uk.bl.wap.util;

/**
 * Comments taken from clamd man page.
 * @author rcoram
 * 
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClamdScanner {
	public static final int DEFAULT_CHUNK_SIZE = 4096; 
	private final static Logger LOGGER = Logger.getLogger( ClamdScanner.class.getName() );
	private String host = "localhost";
	private int port = 3310;
	private int timeout = 0;

	public ClamdScanner() {}

	public ClamdScanner( String host, int port, int timeout ) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
	}

	public int getPort() {
		return this.port;
	}

	/**
	 * @return <CODE>String</CODE> representation of output from clamd
	 */
	public String clamdScan( InputStream input ) {
		Socket socket = new Socket();
		DataOutputStream output = null;
		String result = "";
		try {
			socket.connect( new InetSocketAddress( this.host, this.port ) );
			socket.setSoTimeout( this.timeout );

			output = new DataOutputStream( socket.getOutputStream() );
			byte[] buffer = new byte[ DEFAULT_CHUNK_SIZE ];
			int read = -1;

			// It's recommended to prefix clamd commands with the letter z...to
			// indicate that the command
			// will be delimited by a NULL character...
			output.write( "zINSTREAM\0".getBytes() );
			try {
				while( ( read = input.read( buffer ) ) != -1 ) {
					try {
						// The format of the chunk is: '<length><data>'...
						output.writeInt( read );
						output.write( buffer, 0, read );
						output.flush();
					} catch( IOException e ) {
						LOGGER.log( Level.WARNING, "Error writing to DataOutputStream: " + e.toString() );
						break;
					}
				}
			} catch( IOException e ) {
				LOGGER.log( Level.SEVERE, "Error reading from InputStream: " + e.toString(), e );
			}
			// Streaming is terminated by sending a zero-length chunk.
			output.writeInt( 0 );
			output.flush();
			StringBuffer response = new StringBuffer();
			buffer = new byte [ 1 ];
			while( true ) {
				try {
					socket.getInputStream().read( buffer );
				} catch( IOException e ) {
					LOGGER.log( Level.WARNING, "Error getting result from InputStream: " + e.toString() );
					break;
				}
				// Clamd replies will honour the requested terminator...
				if( buffer[ 0 ] == '\0' )
					break;
				response.append( new String( buffer ) );
			}
			result = response.toString();
		} catch( Exception e ) {
			LOGGER.log( Level.WARNING, "Error connecting to clamd: " + e );
		} finally {
			if( output != null ) {
				try {
					output.close();
				} catch( IOException e ) {
					LOGGER.log( Level.WARNING, "Error closing DataOutputStream: " + e.toString() );
				}
			}
			try {
				socket.close();
			} catch( IOException e ) {
				LOGGER.log( Level.WARNING, "Error closing socket: " + e.toString() );
			}
		}
		return result;
	}
}