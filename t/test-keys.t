#!/usr/bin/perl -I../lib/ -Ilib/

use strict;
use warnings;

use File::Temp qw! tempfile !;
use Test::More tests => 7;

BEGIN
{
    use_ok( "Redis::SQLite", "We could load the module" );
}

# Create a new temporary file
my ( $fh, $filename ) = tempfile();
ok( -e $filename, "The temporary file was created" );
unlink($filename);

# Create a new object
my $redis = Redis::SQLite->new( path => $filename );
isa_ok( $redis, "Redis::SQLite", "Created Redis::SQLite object" );

# We should have zero keys.
is( scalar $redis->keys(), 0, "There are no keys by default" );

# Add some keys
foreach my $x (qw! foo bar baz bart bort bark !)
{
    $redis->set( $x, $x );
}

# Now we should have six keys
is( scalar $redis->keys(), 6, "We've created some keys" );

# We fetch the keys that match the pattern "ba*" and should have two
is( scalar $redis->keys("^ba"), 4, "We filtered them appropriately" );

# But we'll have only one "oo" match.
is( scalar $redis->keys("oo\$"), 1, "We filtered them appropriately, again" );

# Cleanup
unlink($filename);
