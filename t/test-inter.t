#!/usr/bin/perl -I../lib/ -Ilib/

use strict;
use warnings;

use File::Temp qw! tempfile !;
use Test::More tests => 8;

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

# Now we add two sets
$redis->sadd( "english", "Steve" );
$redis->sadd( "english", "Paul" );
$redis->sadd( "english", "Micheal" );
$redis->sadd( "english", "Pete" );

$redis->sadd( "finnish", "Kirsi" );
$redis->sadd( "finnish", "My" );
$redis->sadd( "finnish", "Steve" );
$redis->sadd( "finnish", "Jari" );

# The sets should have three values each.
is( $redis->scard("english"), 4, "The 'english' set has four members" );
is( $redis->scard("finnish"), 4, "The 'finnish' set has four members" );

# The intersection is just one name - Steve
my @combined = $redis->sinter( "english", "finnish" );
is( scalar @combined, 1,       "The union has the expected overlap members" );
is( $combined[0],     "Steve", "Which is what we expect" );

# Cleanup
unlink($filename);