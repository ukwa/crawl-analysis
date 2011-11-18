package uk.bl.wap.fuse;

import java.io.BufferedInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

import fuse.FuseException;
import fuse.FuseFtype;
import fuse.FuseMount;
import fuse.FuseStatfs;
import fuse.compat.Filesystem1;
import fuse.compat.FuseDirEnt;
import fuse.compat.FuseStat;
import fuse.zipfs.util.Node;
import fuse.zipfs.util.Tree;

import static org.archive.io.warc.WARCConstants.*;

public class ArchiveFilesystem implements Filesystem1 {

	private static final Log log = LogFactory.getLog( ArchiveFilesystem.class );
	private static final int blockSize = 512;

	private Tree tree;
	private FuseStatfs statfs;
    private File file;
	//private ArchiveReader archivereader;



	public class ArchiveEntry {
		ArchiveRecordHeader header;
		boolean isDirectory = false;
		long lastModified;
		long offset = -1L;
		int position = -1;
	}

	public ArchiveFilesystem( File file ) {
        this.file = file;
        tree = new Tree();

		try {
			int files = 0;
			int blocks = 0;

			ArchiveEntry dirEntry = new ArchiveEntry();
			dirEntry.header = null;
			dirEntry.isDirectory = true;
			dirEntry.lastModified = ( int ) ( file.lastModified() / 1000L );
			tree.addNode( "/", dirEntry );

            ArchiveReader archivereader = ArchiveReaderFactory.get(file, 0);
			Iterator<ArchiveRecord> iterator = archivereader.iterator();
			while( iterator.hasNext() ) {
				ArchiveRecord record = iterator.next();
				ArchiveEntry archiveEntry = addRecord( record );
				if( archiveEntry != null ) {
					blocks += ( archiveEntry.header.getLength() + blockSize - 1 ) / blockSize;
					this.fixNodes( tree.lookupNode( "/" ), dirEntry );
					files++;
				}
			}
			statfs = new FuseStatfs();
			statfs.blocks = blocks;
			statfs.blockSize = blockSize;
			statfs.blocksFree = 0;
			statfs.files = files;
			statfs.filesFree = 0;
			statfs.namelen = 2048;
			log.info( "Archive file " + file + " structure evaluated: " + files + " URIs, " + blocks + " blocks." );
		} catch( Exception e ) {
			log.error( "ArchiveFilesystem(): " + e.getMessage(), e );
		}
	}

	private ArchiveEntry addRecord( ArchiveRecord record ) throws ParseException {
		if( record instanceof ARCRecord ) {
			ARCRecord arcrecord = ( ARCRecord ) record;
			return addRecord( arcrecord );
		} else {
			WARCRecord warcrecord = ( WARCRecord ) record;
			return addRecord( warcrecord );
		}
	}

	private ArchiveEntry addRecord( WARCRecord record ) throws ParseException {
		ArchiveRecordHeader header = record.getHeader();
		ArchiveEntry warcEntry = new ArchiveEntry();
		String recordType = ( String ) header.getHeaderValue( HEADER_KEY_TYPE );
		if( header.getHeaderFieldKeys().contains( HEADER_KEY_URI ) ) {
			SimpleDateFormat dateformat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
			String path = "/" + header.getHeaderValue( HEADER_KEY_URI ).toString().replace( ":", "/" ).replaceAll( "/+", "/" );
			if( path.endsWith( "/" ) ) {
				path = path + "ROOT";
			}
			if( !recordType.equals( "response" ) ) {
				path = path + "_" + recordType.toUpperCase();
			}
			warcEntry.header = header;
			warcEntry.offset = header.getOffset();
			warcEntry.position = 0;
			warcEntry.lastModified =  ( dateformat.parse( header.getHeaderValue( HEADER_KEY_DATE ).toString() ).getTime() / 1000L );
			tree.addNode( path, warcEntry );
			return warcEntry;
		} else {
			return null;
		}
		
	}


	private ArchiveEntry addRecord( ARCRecord record ) throws ParseException {
		ArchiveRecordHeader header = record.getHeader();
		ArchiveEntry arcEntry = new ArchiveEntry();
		SimpleDateFormat dateformat = new SimpleDateFormat( "yyyyMMddHHmmss" );
		String path = "/" + header.getUrl().replace( ":", "/" ).replaceAll( "/+", "/" );
		if( path.endsWith( "/" ) ) {
			path = path + "ROOT";
		}
		arcEntry.header = header;
		arcEntry.offset = header.getOffset();
		arcEntry.position = 0;
		arcEntry.lastModified =  ( dateformat.parse( header.getDate() ).getTime() / 1000L );
		tree.addNode( path, arcEntry );
		return arcEntry;
	}

	public static void main( String[] args ) {
		if( args.length < 1 ) {
			System.out.println( "Must specify Archive file" );
			System.exit( -1 );
		}

		String fuseArgs[] = new String[ args.length - 1 ];
		System.arraycopy( args, 0, fuseArgs, 0, fuseArgs.length );
		File warcFile = new File( args[ args.length - 1 ] );

		try {
			FuseMount.mount( fuseArgs, new ArchiveFilesystem( warcFile ) );
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}

	@Override
	public FuseStat getattr( String path ) throws FuseException {
		Node node = tree.lookupNode( path );
		ArchiveEntry entry = null;
		if( node == null || ( entry = ( ArchiveEntry ) node.getValue() ) == null )
			throw new FuseException( "No Such Entry" ).initErrno( FuseException.ENOENT );

		FuseStat stat = new FuseStat();
		try {
			stat.nlink = 1;
			stat.uid = 0;
			stat.gid = 0;
			stat.blocks = ( int ) ( ( stat.size + 511L ) / 512L );
			stat.atime = stat.mtime = stat.ctime = (int) entry.lastModified;

			if( entry.isDirectory ) {
				stat.mode = FuseFtype.TYPE_DIR | 0555;
				stat.size = 0;
			} else {
				ArchiveRecordHeader header = entry.header;
				stat.mode = FuseFtype.TYPE_FILE | 0444;
				stat.size = header.getLength();
			}
		} catch( Exception e ) {
			log.error( "getattr(): " + e.toString(), e );
		}

		return stat;
	}

	@Override
	public String readlink( String path ) throws FuseException {
		throw new FuseException( "Not a link" ).initErrno( FuseException.EACCES );
	}

	@Override
	@SuppressWarnings( "unchecked" )
	public FuseDirEnt[] getdir( String path ) throws FuseException {
		Node node = tree.lookupNode( path );
		ArchiveEntry entry = null;

		if( node == null || ( entry = ( ArchiveEntry ) node.getValue() ) == null )
			throw new FuseException( "No Such Entry" ).initErrno( FuseException.ENOENT );

		if( !entry.isDirectory )
			throw new FuseException( "Not a Directory" ).initErrno( FuseException.ENOTDIR );

		Collection<Node> children = node.getChildren();
		FuseDirEnt[] dirEntries = new FuseDirEnt[ children.size() ];

		int i = 0;
		for( Iterator<Node> iter = children.iterator(); iter.hasNext(); i++ ) {
			Node childNode = iter.next();
			ArchiveEntry archiveEntry = ( ArchiveEntry ) childNode.getValue();
			FuseDirEnt dirEntry = new FuseDirEnt();
			dirEntries[ i ] = dirEntry;
			dirEntry.name = childNode.getName();
			if( archiveEntry.isDirectory ) {
				dirEntry.mode = FuseFtype.TYPE_DIR;
			} else {
				dirEntry.mode = FuseFtype.TYPE_FILE;
			}
		}

		return dirEntries;
	}

	@Override
	public void mknod( String path, int mode, int rdev ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void mkdir( String path, int mode ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void unlink( String path ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void rmdir( String path ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void symlink( String from, String to ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void rename( String from, String to ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void link( String from, String to ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void chmod( String path, int mode ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void chown( String path, int uid, int gid ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void truncate( String path, long size ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void utime( String path, int atime, int mtime ) throws FuseException {
		//
	}

	@Override
	public FuseStatfs statfs() throws FuseException {
		return statfs;
	}

	@Override
	public void open( String path, int flags ) throws FuseException {
		if( flags == O_WRONLY || flags == O_RDWR )
			throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );

		Node node = tree.lookupNode( path );
		if( node == null || ( ArchiveEntry ) node.getValue() == null )
			throw new FuseException( "No Such Entry" ).initErrno( FuseException.ENOENT );
	}

	@Override
	public void read( String path, ByteBuffer buf, long offset ) throws FuseException {
		Node node = tree.lookupNode( path );
		ArchiveEntry entry = ( ArchiveEntry ) node.getValue();
		if( !entry.isDirectory ) {
			try {

                //We cannot seek backwards, so create a new reader when we need to read a file
				ArchiveRecord record = ArchiveReaderFactory.get( file, entry.offset ).get();

				if( ( record instanceof ARCRecord ) || entry.header.getHeaderValue( HEADER_KEY_TYPE ).equals( "response" ) ) {
					String url = entry.header.getUrl();
					if( url.matches( "^http.*$" ) ) {
						HttpParser.parseHeaders( record, DEFAULT_ENCODING );
					}
				}
				BufferedInputStream input = new BufferedInputStream( record );
				input.skip( entry.position );

				byte[] bytes = new byte[ buf.capacity() ];
				int n = input.read( bytes );


				if( n > 0 ) {
                    buf.put( bytes, 0, n );
					entry.position += n;
				}
			} catch( Exception e ) {
				log.error( "read(): " + e.toString(), e );
			}
		}
	}

	@Override
	public void write( String path, ByteBuffer buf, long offset ) throws FuseException {
		throw new FuseException( "Read Only" ).initErrno( FuseException.EACCES );
	}

	@Override
	public void release( String path, int flags ) throws FuseException {
		Node node = tree.lookupNode( path );
		if( node == null || ( ArchiveEntry ) node.getValue() == null )
			throw new FuseException( "No Such Entry" ).initErrno( FuseException.ENOENT );

		ArchiveEntry entry = ( ArchiveEntry ) node.getValue();
		entry.position = 0;
	}

	@SuppressWarnings( "unchecked" )
	private void fixNodes( Node root, ArchiveEntry dirEntry ) {
		if( root.isLeafNode() ) {
			return;
		} else {
			Collection<Node> nodes = root.getChildren();
			Iterator<Node> iNodes = nodes.iterator();
			while( iNodes.hasNext() ) {
				Node n = iNodes.next();
				if( n.getValue() == null ) {
					n.setValue( dirEntry );
				}
				this.fixNodes( n, dirEntry );
			}
		}
		return;
	}
}
