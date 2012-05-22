package uk.bl.wap.crawler.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.util.ClamdScanner;
import uk.bl.wap.util.ClamdScannerPool;

/**
 * 
 * @author rcoram
 */

public class ViralContentProcessor extends Processor {
	private final static Logger LOGGER = Logger.getLogger( ViralContentProcessor.class.getName() );
	private static final long serialVersionUID = -321505737175991914L;
	private int virusCount = 0;
	private ClamdScannerPool pool;

	public ViralContentProcessor() {}

	/**
	 * The host machine on which clamd is running.
	 */
	{
		setClamdHost( "localhost" );
	}
	@Autowired
	public void setClamdHost( String clamdHost ) {
		kp.put( "clamdHost", clamdHost );
	}

	private String getClamdHost() {
		return ( String ) kp.get( "clamdHost");
	}

	/**
	 * The list of ports on which instances of clamd can be found.
	 */
	{
		setClamdPortList( new ArrayList<Integer>() );
	}
	@SuppressWarnings( "unchecked" )
	public List<Integer> getClamdPortList() {
		return ( List<Integer> ) kp.get( "clamdPortList" );
	}
	@Autowired
	public void setClamdPortList( List<Integer> ports ) {
		kp.put( "clamdPortList", ports );
	}

	/**
	 * The timeout in milliseconds for clamd.
	 */
	{
		setClamdTimeout( 10000 );
	}
	@Autowired
	public void setClamdTimeout( int clamdTimeout ) {
		kp.put( "clamdTimeout", clamdTimeout );
	}

	private int getClamdTimeout() {
		return ( Integer ) kp.get( "clamdTimeout");
	}
	
	/**
	 * Initialise the pool of clamd instances.
	 */
	{
		this.pool = new ClamdScannerPool( this.getClamdHost(), this.getClamdPortList(), this.getClamdTimeout() );
	}

	@Override
	protected void innerProcess( CrawlURI curi ) throws InterruptedException {
		ClamdScanner scanner = null;
		try {
			if( this.pool == null ) {
				this.pool = new ClamdScannerPool( this.getClamdHost(), this.getClamdPortList(), this.getClamdTimeout() );
			} else {
				if( this.pool.getPoolSize() == 0 ) {
					this.pool.add( this.getClamdHost(), this.getClamdPortList(), this.getClamdTimeout() );
				}
			}
			scanner = ( ClamdScanner ) pool.borrow();
			String result = scanner.clamdScan( curi.getRecorder().getReplayInputStream() );
			if( result.matches( "^stream:.+$" ) ) {
				if( ! result.matches( "^stream: OK$" ) ) {
					curi.getAnnotations().add( result );
					virusCount++;
				}
			} else {
				throw new Exception( result );
			}
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, e.toString() );
		} finally {
			if( scanner != null ) {
				pool.putBack( scanner );
			}
		}
	}

	@Override
	protected boolean shouldProcess( CrawlURI uri ) {
		return uri.isHttpTransaction();
	}

	@Override
	public String report() {
		StringBuffer report = new StringBuffer();
		report.append( super.report() );
		report.append("  Streams scanned: " + this.getURICount() + "\n" );
		report.append("  Viruses found:   " + this.virusCount + "\n" );

		return report.toString();
	}
}