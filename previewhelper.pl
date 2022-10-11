#!/usr/local/bin/perl

# Preview Generator Helper for Nextcloud
# Seking the latest file id in an external storage, then
# job queues for Preview Generator are inserted on the table.

use strict;
use DBI;

# Config. to connect MySQL DB
our $DB_NAME = "nextcloud";
our $DB_USER = "dbadmin";
our $DB_PASS = "password";
our $DB_HOST = "localhost";
our $DB_PORT = "3386";
# Nextcloud - to specify a storage to seek
our $NC_USER = "nextuser";  # User name who can read the storage
our $NC_STORAGE_ID = 7;		# Storage ID which is found on oc_storages
# To record the last file_id to seek
our $LAST_ID_FILE = "/usr/local/www/last_file_id.dat";


# main
my $lastFileID = &readPreviousFileId;
if(  $lastFileID < 0 ) {
	# The first run - save the last file_id and quit
	&writePreviousFileId( &getLastFileId );
} else {
	# Insert file_id(s) into the job queue table of Preview generator
	&setPreviewQueue( $lastFileID );
	&writePreviousFileId( &getLastFileId );
}	

# To get the last file_id at the previous execution.
# return filed_id which is saved on the file
#			-1 : no data. This means the first execution of this script.
sub readPreviousFileId {
	my $retVal;
	if( !-f $LAST_ID_FILE )  { return -1; }
	if( !open( IN, "<$LAST_ID_FILE" ) ) {
		die "Cannot open the file. $LAST_ID_FILE\n";
	} else {
		my @line = <IN>;
		close( IN );
		$retVal = $line[ 0 ];
	}
	return $retVal;
}

# To record the last file_id on the file
sub  writePreviousFileId {
	my $fileId = shift;
	$fileId //= 0;
	if( !open( OUT, ">$LAST_ID_FILE" ) ) {
		die "Cannot write to $LAST_ID_FILE\n";
	} 
	print OUT "$fileId";
	close( OUT );
}


# To connect to the DB
sub connectDB {	
	return DBI->connect( "dbi:mysql:dbname=$DB_NAME; host=$DB_HOST; port=$DB_PORT", "$DB_USER", "$DB_PASS" ) or die "$!\n Failed to connect with DB.\n";
}


# To GET the last (= biggest) fileid form oc_filecache
sub getLastFileId {
	my $dbh = &connectDB; 
	my $ary_row = $dbh->selectrow_arrayref( "SELECT MAX(fileid) FROM oc_filecache WHERE storage=${NC_STORAGE_ID} AND mimetype!=2;" );
	$dbh->disconnect;
	return $ary_row->[0];
}


# To insert queues to Preview Generator
# $startFileId  : the start point of file_id  to add the queue
sub setPreviewQueue {
	my $startFileId = shift;
	my $dbh = &connectDB; 
	my $sth = $dbh->prepare( 
		"INSERT oc_preview_generation (uid, file_id) SELECT '${NC_USER}', fileid FROM oc_filecache WHERE storage=${NC_STORAGE_ID} AND fileid>${startFileId} AND mimetype!=2 AND NOT path LIKE '%.DS_Store'"
		);
	$sth->execute();
	$dbh->disconnect;
}

