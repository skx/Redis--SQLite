#!/usr/bin/perl -I../lib/ -Ilib/

use strict;
use warnings;

use File::Temp qw! tempfile !;
use Test::More tests => 6;

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

# Run five increment options
for ( my $i = 0 ; $i < 5 ; $i++ )
{
    $redis->incr("foo");
}

# We should have one key now.
is( scalar $redis->keys(), 1, "There is now a single key" );

# Which should have the value five
is( $redis->get("foo"), 5, "We have the correct value" );


# Cleanup
unlink($filename);
