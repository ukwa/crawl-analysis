#!/bin/sh

# Usage: warcfs_mount.sh file.warc.gz /mount/point/ (-d)

(
	FUSE_HOME="$(dirname "$0")"
	
	[[ "$3" == "-d" ]] && DEBUG="-Dorg.apache.commons.logging.Log=fuse.logging.FuseLog -Dfuse.logging.level=DEBUG"
	
	LD_LIBRARY_PATH=$FUSE_HOME/jni java \
		-cp "$FUSE_HOME/build:$FUSE_HOME/lib/*" \
		$DEBUG uk.bl.wap.fuse.WARCFilesystem \
		-f -s -o allow_other "$2" "$1"
) &
