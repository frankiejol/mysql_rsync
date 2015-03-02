#!/usr/bin/perl

use warnings;
use strict;

use DBI;
use IPC::Run qw(run);
use Getopt::Long;
use Term::ReadKey;


my %TABLE;

my $LIMIT = 10000;
my $DRY_RUN;

my ($SRC_HOST,$SRC_DB,$SRC_USER,$SRC_PASS) = ('localhost');
my ($DST_HOST,$DST_DB,$DST_USER,$DST_PASS) = ('localhost');

my $MYSQLDUMP = `which mysqldump`;
chomp $MYSQLDUMP;

my ($USAGE) = $0 =~ m{.*/(.*)};
$USAGE = $0 if !$USAGE;

$USAGE .=" [--help] [--verbose] [--mysqldump=$MYSQLDUMP]"
    ." [--limit=$LIMIT] [--dry-run]"
    ." [--src-host=$SRC_HOST] --src-db=DB"
    ." [--src-user=username] [--src-pass=pass] [--dst-host=HOST] [--dst-db=DB]"
    ." [--dst-user=username] [--dst-pass=pass]"
    ." [--P]"
    ." [table1 ... tablen]";

my $help;
my ($VERBOSE, $DEBUG, $ASK_PASSWORD);
GetOptions(
               help => \$help
             ,debug => \$DEBUG
          ,verbose  => \$VERBOSE
       ,'dry-run|n' => \$DRY_RUN
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
    ,           P  => \$ASK_PASSWORD
) or exit -1;

$DST_DB = $SRC_DB	if $SRC_HOST ne $DST_HOST && !$DST_DB;

if ( $ASK_PASSWORD && ($SRC_PASS || $DST_PASS) ) {
    warn "ERROR: --P will ask for password, do not put it in the arguments\n"
        ."$USAGE\n";
    exit -1;
}
if ( $help || !$SRC_DB || !$DST_DB ) {
    print "$USAGE\n";
    exit;
}
ask_password() if $ASK_PASSWORD;

$DST_PASS = $SRC_PASS	if $SRC_HOST eq $DST_HOST && !$DST_PASS;
$DST_USER = $SRC_USER	if $SRC_HOST eq $DST_HOST && !$DST_USER;

$|=1;

my $dbh_src = DBI->connect("DBI:mysql:host=$SRC_HOST;database=$SRC_DB"
    ,$SRC_USER, $SRC_PASS,{ RaiseError => 1, PrintError => 0})
        or exit -2;
my $dbh_dst = DBI->connect("DBI:mysql:host=$DST_HOST;database=$DST_DB"
    ,$DST_USER, $DST_PASS,{RaiseError => 1, PrintError => 0 })
        or exit -2;

##########################################

sub ask_password {
    print "Enter source password :";
    ReadMode('noecho');
    $SRC_PASS = ReadLine(0);
    chomp $SRC_PASS;

    print "\nEnter destination password :";
    $DST_PASS = ReadLine(0);
    chomp $DST_PASS;
    ReadMode(0);
}

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
    my ($dbh , $table, $row, $update) = @_;

    my $query = "INSERT INTO $table ("
        .join(" , ",sort keys %$row)." ) "
        ." VALUES ( "
        .join(" , ",map { '?' } keys %$row)
        ." )";

    if ($update) {
        $query = "UPDATE $table SET "
            .join(" , ",map { "$_=?" } sort keys %$row)
            ." WHERE $TABLE{$table}->{id}=?";
    }
    print "$query\n"    if $DEBUG;
    my $sth = $dbh->prepare($query);
    insert_row($sth,$row, $update, $table);
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
            if ($row->{Key} && $row->{Null} eq 'NO'
                    && $row->{Type} eq 'timestamp') {
                $TABLE{$table}->{timestamp} = $row->{Field};
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
    my ($sth_insert, $row, $update, $table) = @_;
    print "INSERT ROW ".join(" ",%$row)."\n" if $DEBUG;
    return if $DRY_RUN;

    eval {
        my @row = map { $row->{$_} } sort keys %$row;
        push @row,($row->{$TABLE{$table}->{id}})    if $update;
        $sth_insert->execute(@row ) ;
    };
    if ($@ && $@ !~ /Duplicate entry/) {
        warn $sth_insert->err." ".$sth_insert->errstr." ".$@;
        exit;
    }

}

sub dump_wild {
    my ($table, $field, $update) = @_;

    my $id = maxim_id($dbh_dst,$table, $field);
    $id = 0 if !defined $id;
	
    my $time0 = time;
    my $n = 0;
    my $old_id=0;

    print "dump wild $table\n"  if $VERBOSE;
    my $max = maxim_id($dbh_src,$table,$field);
    my $n0 = $max - $id if $max && $id =~ /^\d+$/;
    for (;;) {
        $id = 0 if !defined $id;
        my $sth = $dbh_src->prepare("SELECT * FROM $table "
        ." WHERE $field >= ? "
        ." LIMIT ".($LIMIT+1));
        $sth->execute($id);
        $old_id = $id;

        my $row = $sth->fetchrow_hashref or do {
            print "finish\n"    if $VERBOSE;
            return ;
        };

        my $sth_insert = sth_insert($dbh_dst, $table, $row, $update);
        $dbh_dst->do("SET autocommit=0");
        while ( $row = $sth->fetchrow_hashref ) {
            insert_row($sth_insert, $row, $update, $table);
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
            $id = $row->{$field};
        }
        $sth_insert->finish;
        $sth ->finish;
        $dbh_dst->do("COMMIT");
#        return if $id <= $old_id or $n < $LIMIT;
        return if $n < $LIMIT;
    }
}

###########################################

search_tables(\@ARGV);

for my $table ( sort keys %TABLE) {
    print "$table\n";
    dump_wild($table,$TABLE{$table}->{id})              if $TABLE{$table}->{id};
    dump_wild($table, $TABLE{$table}->{timestamp},1)    if $TABLE{$table}->{timestamp};
}

1;
