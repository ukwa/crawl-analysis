package uk.bl.wap.crawler.processor;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.util.ClamdScanner;

/**
 * 
 * @author rcoram
 */

public class ViralContentProcessor extends Processor {
	private final static Logger LOGGER = Logger.getLogger( ViralContentProcessor.class.getName() );
	private static final long serialVersionUID = -321505737175991914L;
	private int virusCount = 0;
	private ClamdScanner scanner;

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
		setClamdPort( 3310 );
	}
	public int getClamdPort() {
		return ( Integer ) kp.get( "clamdPort" );
	}
	@Autowired
	public void setClamdPort( int port ) {
		kp.put( "clamdPort", port );
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
		this.scanner = new ClamdScanner( this.getClamdHost(), this.getClamdPort(), this.getClamdTimeout() );
	}

	@Override
	protected void innerProcess( CrawlURI curi ) throws InterruptedException {
		try {
			if( this.scanner == null ) {
				this.scanner = new ClamdScanner( this.getClamdHost(), this.getClamdPort(), this.getClamdTimeout() );
			}
			String result = scanner.clamdScan( curi.getRecorder().getReplayInputStream() );
			if( result.matches( "^([1-2]:\\s+)?stream:.+$" ) ) {
				if( ! result.matches( "^([1-2]:\\s+)?stream: OK.*$" ) ) {
					curi.getAnnotations().add( result );
					virusCount++;
				}
			} else {
				throw new Exception( result );
			}
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, e.toString() );
		}
	}

	@Override
	protected boolean shouldProcess( CrawlURI uri ) {
		return uri.is2XXSuccess();
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