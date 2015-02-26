use strict;
use warnings;
use Cwd;
use DBI;
use Test::More;

my $TABLE_TEST = 'table_rsync1';
my $SRC_DB = "test_rsync_src";
my $DST_DB = "test_rsync_dst";

my ($SRC_DBH, $DST_DBH);

my $dir = cwd();
my $MYSQL_RSYNC = "$dir/mysql_rsync.pl";

sub create_table_test {
    my ($dbh) = @_;
    my $sth;
    $sth = $dbh->do("DROP TABLE $TABLE_TEST"); 
    $sth = $dbh->do("CREATE TABLE $TABLE_TEST "
        ."(id int auto_increment primary key, name char(20))");
}

sub drop_table_test {
    my ($dbh) = @_;
    my $sth = $dbh->do("DROP TABLE $TABLE_TEST"); 
}


sub select_row {
    my $dbh = shift;
    my %value = @_;
    my $where = '';
    for (sort keys %value) {
        $where .= "$_ = ? ";
    }
    my $query = "SELECT * FROM $TABLE_TEST WHERE $where";
    my $sth = $dbh->prepare($query);
    $sth->execute(map { $value{$_} } sort keys %value);
    my $row = $sth->fetchrow_hashref;
    $sth->finish;
    return $row;
}

sub insert_row {
    my $dbh = shift;
    my %value = @_;
    my $fields = join(",",sort keys %value);
    my $values = join(",",map { '?' } keys %value);

    ok(!select_row($dbh,@_),"@_ should not be there") or return;
    my $query = "INSERT INTO $TABLE_TEST ($fields) VALUES ($values) ";
    my $sth = $dbh->prepare($query);
    $sth->execute(map { $value{$_} } sort keys %value);
    ok(!$dbh->err,$dbh->errstr);
    $sth->finish;
    ok(select_row($dbh,@_),"I can't find @_ ");
}

sub run_rsync {
    my $table = shift;
    my @cmd = ($MYSQL_RSYNC
                ,"--src-db=$SRC_DB"
                ,"--dst-db=$DST_DB"
                ,$table);
    my $cmd=join(" ",@cmd);
    my $out= `$cmd`;
#    warn $cmd;
#    warn $out;
    ok(!$?,"ERROR $? en @cmd");
}

sub test_simple {
    create_table_test($SRC_DBH);
    skip($SRC_DBH->err.":".$SRC_DBH->errstr,3) if $SRC_DBH->err;

    my %value = (name => 'pepe');
    insert_row($SRC_DBH, %value);

    drop_table_test($DST_DBH);
    ok(!select_row($DST_DBH,%value));

    run_rsync($TABLE_TEST);
    ok(select_row($DST_DBH,%value));
}

sub test_inc_one {
    test_simple();
    my %value = (name => 'foo');
    ok(!select_row($DST_DBH,%value));

    insert_row($SRC_DBH, %value);
    run_rsync($TABLE_TEST);
    ok(select_row($DST_DBH,%value));

}

SKIP: {
    eval { $SRC_DBH = DBI->connect("DBI:mysql:$SRC_DB",undef,undef
            ,{RaiseError => 0, PrintError => 0}) };
    skip("Can't connect to $SRC_DB",3) if !$SRC_DBH || $@ || $SRC_DBH->err;
    ok($SRC_DBH);

    eval { $DST_DBH = DBI->connect("DBI:mysql:$DST_DB",undef,undef
            ,{RaiseError => 0, PrintError => 0}) };
    ok($DST_DBH);
    skip("Can't connect to $DST_DB",3) if !$DST_DBH|| $@ || $DST_DBH->err;

    test_simple();
    test_inc_one();
};

done_testing();
