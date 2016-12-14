#!/usr/bin/perl -I../lib/ -Ilib/

use strict;
use warnings;

use File::Temp qw! tempfile !;
use Test::More tests => 9;

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
is( $redis->strlen( "greet"), 5, "The key is the correct length" );

# Now we append
$redis->append( "greet", ", world" );
is( scalar $redis->keys(), 1, "There is still only a single key" );

# Fetching the value should result in 'Hello, world'
is( $redis->get( "greet" ), "Hello, world" , "Appending worked" );

# WHich means the size is bigger.
is( $redis->strlen( "greet" ), 12, "The appended key is longer" );

# Cleanup
unlink($filename);
