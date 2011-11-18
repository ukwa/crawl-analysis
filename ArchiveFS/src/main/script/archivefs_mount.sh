#!/bin/bash

# Usage: archivefs_mount.sh file.(w)arc.gz /mount/point/ (-d)

(
	FUSE_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
	
	[[ "$3" == "-d" ]] && DEBUG="-Dorg.apache.commons.logging.Log=fuse.logging.FuseLog -Dfuse.logging.level=DEBUG"
	
	LD_LIBRARY_PATH=$FUSE_HOME/jni java \
		-cp "$FUSE_HOME/lib/*" \
		$DEBUG uk.bl.wap.fuse.ArchiveFilesystem \
		-f -s -o allow_other "$2" "$1"
) &
