#!/usr/bin/perl

use warnings;
use strict;

use DBI;
use IPC::Run qw(run);
use Getopt::Long;

my %TABLE;

my $LIMIT = 10000;

my ($SRC_HOST,$SRC_DB,$SRC_USER,$SRC_PASS) = ('localhost');
my ($DST_HOST,$DST_DB,$DST_USER,$DST_PASS) = ('localhost');

my $MYSQLDUMP = `which mysqldump`;
chomp $MYSQLDUMP;

my ($USAGE) = $0 =~ m{.*/(.*)};
$USAGE = $0 if !$USAGE;

$USAGE .=" [--help] [--verbose] [--mysqldump=$MYSQLDUMP] [--src-host=$SRC_HOST] --src-db=DB"
    ." [--src-user=username] [--src-pass=pass] [--dst-host=HOST] [--dst-db=DB]"
    ." [--dst-user=username] [--dst-pass=pass]"
    ." [table1 ... tablen]";

my $help;
my ($VERBOSE, $DEBUG);
GetOptions(
               help => \$help
             ,debug => \$DEBUG
          ,verbose  => \$VERBOSE
    ,     mysqldump => \$MYSQLDUMP
    ,     'limit=s' => \$LIMIT
    ,  'src-host=s' => \$SRC_HOST
    ,    'src-db=s' => \$SRC_DB
    ,  'src-user=s' => \$SRC_USER
    ,  'src-pass=s' => \$SRC_PASS
    ,  'dst-host=s' => \$DST_HOST
    ,    'dst-db=s' => \$DST_DB
    ,  'dst-user=s' => \$DST_USER
    ,  'dst-pass=s' => \$DST_PASS
) or exit -1;

$DST_DB = $SRC_DB	if $SRC_HOST ne $DST_HOST && !$DST_DB;
$DST_PASS = $SRC_PASS	if $SRC_HOST eq $DST_HOST && !$DST_PASS;

if ( $help || !$SRC_DB || !$DST_DB ) {
    print "$USAGE\n";
    exit;
}

$|=1;

my $dbh_src = DBI->connect("DBI:mysql:host=$SRC_HOST;database=$SRC_DB"
    ,$SRC_USER, $SRC_PASS);
my $dbh_dst = DBI->connect("DBI:mysql:host=$DST_HOST;database=$DST_DB"
    ,$DST_USER, $DST_PASS,{RaiseError => 1, PrintError => 0 });

##########################################


sub create_table {
    my $table = shift;
    my @cmd = ($MYSQLDUMP
        ,'--skip-lock-tables'
        ,"-h",$SRC_HOST);
    push @cmd,('-u',$SRC_USER ) if $SRC_USER;
    push @cmd,("-p$SRC_PASS" ) if $SRC_PASS;
    push @cmd,('-d',$SRC_DB,$table);
    print join (" ",@cmd)."\n"  if $VERBOSE;
    my ($in,$out,$err);
    run(\@cmd, \$in, \$out, \$err);
#    print $out if $out;
    print $err if $err;

    @cmd = ('mysql',"-h",$DST_HOST,$DST_DB);
    push @cmd,('-u',$DST_USER ) if $DST_USER;
    push @cmd,("-p$DST_PASS" ) if $DST_PASS;

#    warn join (" ",@cmd)."\n";
    my ($out2,$err2);
    run(\@cmd, \$out, \$out2, \$err2);
    print $out2 if $out2;
    print $err2 if $err2;
#    exit;
}
sub maxim_id {
    my ($dbh,$table,$id) = @_;
    my $query = "SELECT max($id) FROM $table";
    print "$query\n"    if $VERBOSE;
    my ($sth , $max_id);
    eval {
        $sth = $dbh->prepare($query);
        $sth->execute;
        ($max_id) = $sth->fetchrow;
        $max_id = 0 if !defined $max_id;
        print "\t$max_id\n"   if $VERBOSE;
        $sth->finish;
    };
    create_table($table) if $@ && $@ =~ /Table.*doesn't exist/;
    return $max_id;
}

sub sth_insert {
    my ($dbh , $table, $row) = @_;
    my $query = "INSERT INTO $table ("
        .join(" , ",sort keys %$row)." ) "
        ." VALUES ( "
        .join(" , ",map { '?' } keys %$row)
        ." )";
    my $sth = $dbh->prepare($query);
    insert_row($sth,$row);
    return $sth;
}

sub search_tables {
    my $tables_list = shift;

    $tables_list = undef if !scalar@$tables_list;
    my %table_req;
    %table_req = map { $_ => 1 } @$tables_list  if $tables_list;

    my $sth = $dbh_src->prepare("SHOW TABLES");
    $sth->execute;
    while (my ($table) = $sth->fetchrow) {
        next if $tables_list && !$table_req{$table};

        my $sth_desc = $dbh_src->prepare("DESC $table");
        $sth_desc->execute;
        while (my $row = $sth_desc->fetchrow_hashref ) {
            if ($row->{Key} && $row->{Key} eq 'PRI' ) {
#                 print "$table : $row->{Field}\n";
                 $TABLE{$table}->{id} = $row->{Field};
            }
#            for (sort keys %$row) {
#                print "$_: $row->{$_}\n" if defined $row->{$_}
#                                            && $row->{$_};
#            }
#            print "\n";
        }
        $sth_desc->finish;
    }
    $sth->finish;
}

sub insert_row {
    my ($sth_insert, $row) = @_;
    eval {
        $sth_insert->execute(map { $row->{$_} } sort keys %$row) ;
    };
    if ($@ && $@ !~ /Duplicate entry/) {
        warn $sth_insert->err." ".$sth_insert->errstr." ".$@;
        exit;
    }

}

sub dump_wild {
    my ($table,$id) = @_;
    $id = 0 if !defined $id;
	
    my $time0 = time;
    my $n = 0;
    my $old_id=0;

    print "dump wild $table\n"  if $VERBOSE;
    my $max = maxim_id($dbh_src,$table,$TABLE{$table}->{id});
    my $n0 = $max - $id if $max;
    for (;;) {
        $id = 0 if !defined $id;
        my $sth = $dbh_src->prepare("SELECT * FROM $table "
        ." WHERE $TABLE{$table}->{id} >= ? "
        ." LIMIT ".($LIMIT+1));
        $sth->execute($id);
        $old_id = $id;

        my $row = $sth->fetchrow_hashref or do {
            print "finish\n"    if $VERBOSE;
            return ;
        };

        my $sth_insert = sth_insert($dbh_dst, $table, $row);
        $dbh_dst->do("SET autocommit=0");
        while ( $row = $sth->fetchrow_hashref ) {
            print "going to insert $table $TABLE{$table} : "
                ."$row->{$TABLE{$table}} ? \n" if $DEBUG;
            insert_row($sth_insert, $row);
            $n++;
            if ( time - $time0 > 10) {
                $time0 = time;
                my $pc='';
                if ( $n0 ) {
                    $pc = $n/$n0*100;
                    $pc =~ s/(\d+\.\d\d).*/$1/;
                    $pc .= " %";
                }
                print "$n inserted in $table $pc \n";
            }
            $id = $row->{$TABLE{$table}->{id}};
        }
        $sth_insert->finish;
        $sth ->finish;
        $dbh_dst->do("COMMIT");
        return if $id <= $old_id or $n < $LIMIT;
    }
}

###########################################

search_tables(\@ARGV);

for my $table ( sort keys %TABLE) {
    print "$table\n";
    my $id = maxim_id($dbh_dst,$table, $TABLE{$table}->{id});
    dump_wild($table,$id);
}

1;
