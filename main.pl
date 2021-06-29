use strict;
use warnings;
use Data::Dumper;
use Getopt::Std;
use XML::Simple;

use DBI;

$| = 1;

sub main{
    my %opts;
    getopts('i:e',\%opts);
    if(!checkusage(\%opts)){
        usage();
        exit();
    }
    my $dbh = DBI->connect("dbi:mysql:db_one", "splunk_user", "password");
    unless(defined($dbh)){
        die "cannot connect to db"
    }
	if ( $opts{"i"} ) {
		my $input_dir = $opts{"i"};

		my @files = get_files($input_dir);
		my @data = process_files( \@files, $input_dir );
		print "Found " . scalar(@files) . " files\n";
        print Dumper(@data);
        add_to_database($dbh, \@data);
	}
    if($opts{"e"}){
        export_from_database($dbh);
    }
    $dbh->disconnect();

	print "Completed.\n";
}
sub export_from_database{
    my $dbh = shift;
    
    my $sql = 'select b.id as band_id, b.name as band_name, a.id as album_id, ' .
		'a.name as album_name, a.position as position  ' .
		'from bands b join albums a on a.band_id=b.id;';
    my $sth = $dbh->prepare($sql);
    unless(defined($sth)){
        die "unable to prepare export query.\n";
    }
    unless($sth->execute()){
        die "unable to execute query\n";
    }
    while(my $row = $sth->fetchrow_hashref()){
        my $band_id = $row->{"band_id"};
        my $band_name = $row->{"band_name"};
        my $album_id = $row->{"album_id"};
        my $album_name = $row->{"album_name"};
        my $position = $row->{"position"};
        print "$band_id, $band_name, $album_id, $album_name, $position\n";
    }
    $sth->finish();
}
sub add_to_database{
    my ($dbh, $data) = @_;
    my $sth_bands = $dbh->prepare('insert into bands (name) values(?)');
    unless($sth_bands){
        die "error preparing band insert sql";
    }
    my $sth_albums = $dbh->prepare('insert into albums (name, position, band_id) values(?,?,?)');
    unless($sth_albums){
        die "error preparing band insert sql";
    }
    #print dumper($data);
    foreach my $data(@{$data}){
        my $band_name = $data->{"band_name"};
        my $albums = $data->{"albums"};
        unless($sth_bands->execute($band_name)){
            die("cant insert into table bands")
        }
        print("\ninserted into db: $band_name\n");

        my $band_id = $sth_bands->{'mysql_insertid'};
        foreach my $album(@{$albums}){
            my $album_name = $album->{"name"};
            my $position_name = $album->{"position"};
            unless($sth_albums->execute($album_name, $position_name, $band_id)){
                die "unable to execute albums insert.\n"
            }
        }

    }
    $sth_bands->finish();
	$sth_albums->finish();

}
sub process_files{
    my ($files, $input_dir) = @_;
    my @data;

    foreach my $file(@{$files}){
        push @data, process_file($file, $input_dir)
    }
    return @data;
}
sub process_file{
    my ($file, $input_dir) = @_;
    print("processing file: $file in $input_dir");
    my $filepath = "$input_dir/$file";
    open(INPUTFILE, $filepath) or die "unable to open $filepath";
    undef $/;

    my $content = <INPUTFILE>;
    close(INPUTFILE);

    my $parser = new XML::Simple;
    my $dom = $parser->XMLin($content, ForceArray=>1);

    my @output;
    foreach my $entry(@{$dom->{"entry"}}){
        my $band_name = $entry->{"band"}[0];
        #print Dumper($entry);

        my @albums;
        foreach my $album(@{$entry->{"album"}}){
            my $album_name = $album->{"name"}[0];
            my $chart_position = $album->{"chartposition"}[0];
            push @albums,{
                "name"=>$album_name,
                "position"=>$chart_position,
            };
        }
        push @output,{
            "band_name"=>$band_name,
            "albums"=>\@albums,
        };
    }
    return @output;
}

sub get_files{
    my $input_dir = shift;

    unless(opendir(INPUTDIR, $input_dir)){
        die "\u unable to open directory '$input_dir'\n";
    }
    my @files = readdir(INPUTDIR);
    closedir(INPUTDIR);

    @files = grep(/\.xml$/i, @files);
    return @files;
}
sub checkusage{
    my $opts = shift;

    my $i = $opts->{"i"};
    my $e = $opts->{"e"};

    unless(defined($i) or defined ($e)){
        return 0;
    }
    return 1;
}
sub usage {
	print <<USAGE;
	
usage: perl main.pl <options>
	-i <directory>	import data; specify directory in which to find XML files.
	-e export data from database

example usage:
	# Process files in currect directory.
	perl main.pl -i
	perl main.pl -e
	
USAGE
}

main();