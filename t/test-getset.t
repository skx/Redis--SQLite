#!/usr/bin/perl -I../lib/ -Ilib/

use strict;
use warnings;

use File::Temp qw! tempfile !;
use Test::More tests => 10;

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

# Now we set a greeting.
$redis->set( "greet", "Hello" );

# Which will result in a single key.
is( scalar $redis->keys(), 1, "There is now a single key" );

# Of five bytes in length.
is( $redis->strlen("greet"), 5, "The key is the correct length" );

# Now we'll change it
my $out = $redis->getset( "greet", "Moi" );

is( $out,                  "Hello", "getset returned the previous value" );
is( scalar $redis->keys(), 1,       "There is still only a single key" );
is( $redis->get("greet"),  "Moi",   "The new value was stored as expected" );
is( $redis->strlen("greet"), 3, "The updated value has the right length" );

# Cleanup
unlink($filename);
