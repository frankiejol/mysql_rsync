use Test::More;
use Cwd;

my $dir = cwd();
my $mysql_rsync = "$dir/mysql_rsync.pl";

ok(-e $mysql_rsync,"Missing $mysql_rsync")  or BAIL_OUT("Missing $mysql_rsync");

system($mysql_rsync,'--help');
ok(!$?,"Exit code $?")                      or BAIL_OUT("$mysql_rsync unusable");

done_testing();
