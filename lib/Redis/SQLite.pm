
=head1 NAME

Redis::SQLite - Redis API-compatible storage system using SQLite.

=cut

=head1 SYNOPSIS

=for example begin

    #!/usr/bin/perl -w

    use Redis::SQLite;
    use strict;

    my $db = Redis::SQLite->new();

    $db->set( "foo", "bar" );

    print $db->get( "foo" ) . "\n";

=for example end


=head1 DESCRIPTION

This package is an implementation of the L<Redis> Perl-client API, which
stores all data in an SQLite database.

It is B<not> a drop-in replacement, because it doesn't implement all the
features you'd expect from the real Redis module.  Just enough to be useful.

=cut

=head1 METHODS

=cut


package Redis::SQLite;



use strict;
use warnings;
use DBI;


our $VERSION = '0.1';


=head2 new

Constructor

=cut

sub new
{
    my ( $proto, %supplied ) = (@_);
    my $class = ref($proto) || $proto;

    my $self = {};
    bless( $self, $class );

    my $file = $supplied{ 'path' } || $ENV{ 'HOME' } . "/.predis.db";
    my $create = 1;
    $create = 0 if ( -e $file );

    $self->{ 'db' } =
      DBI->connect( "dbi:SQLite:dbname=$file", "", "", { AutoCommit => 1 } );

    #
    #  Create teh database if it is missing.
    #
    if ($create)
    {
        $self->{ 'db' }->do(
             "CREATE TABLE string (id INTEGER PRIMARY KEY, key UNIQUE, val );");
        $self->{ 'db' }
          ->do("CREATE TABLE sets (id INTEGER PRIMARY KEY, key, val );");
    }

    #
    #  This is potentially risky, but improves the throughput by several
    # orders of magnitude.
    #
    if ( !$ENV{ 'SAFE' } )
    {
        $self->{ 'db' }->do("PRAGMA synchronous = OFF");
        $self->{ 'db' }->do("PRAGMA journal_mode = MEMORY");
    }

    return $self;
}


=head2 get

Get the value of a string-key.

=cut

sub get
{
    my ( $self, $key ) = (@_);

    if ( !$self->{ 'get' } )
    {
        $self->{ 'get' } =
          $self->{ 'db' }->prepare("SELECT val FROM string WHERE key=?");
    }
    $self->{ 'get' }->execute($key);
    my $x = $self->{ 'get' }->fetchrow_array() || undef;
    $self->{ 'get' }->finish();
    return ($x);
}



=head2 set

Set the value of a string-key.

=cut

sub set
{
    my ( $self, $key, $val ) = (@_);

    if ( !$self->{ 'ins' } )
    {
        $self->{ 'ins' } =
          $self->{ 'db' }
          ->prepare("INSERT OR REPLACE INTO string (key,val) VALUES( ?,? )");
    }
    $self->{ 'ins' }->execute( $key, $val );
    $self->{ 'ins' }->finish();

}



=head2 incr

Increment and return the value of an (integer) string-key.

=cut

sub incr
{
    my ( $self, $key, $amt ) = (@_);

    $amt = 1 if ( !defined($amt) );

    my $cur = $self->get($key) || 0;
    $cur += $amt;
    $self->set( $key, $cur );

    return ($cur);
}



=head2 decr

Decrement and return the value of an (integer) string-key.

=cut

sub decr
{
    my ( $self, $key, $amt ) = (@_);

    $amt = 1 if ( !defined($amt) );

    my $cur = $self->get($key) || 0;
    $cur -= $amt;
    $self->set( $key, $cur );

    return ($cur);
}



=head2 del

Delete a given key, regardless of whether it holds a string or a set.

=cut

sub del
{
    my ( $self, $key ) = (@_);

    # strings
    my $str = $self->{ 'db' }->prepare("DELETE FROM string WHERE key=?");
    $str->execute($key);
    $str->finish();

    # sets
    my $set = $self->{ 'db' }->prepare("DELETE FROM sets WHERE key=?");
    $set->execute($key);
    $set->finish();

}


=head2 keys

Get known-keys.  These can be optionally filtered by a (perl) regular
expression.

=cut

sub keys
{
    my ( $self, $pattern ) = (@_);

    # Get all keys into this hash
    my %known;

    # We run the same query against two tables.
    foreach my $table (qw! string sets !)
    {
        # Get the names of the key.
        my $str = $self->{ 'db' }->prepare("SELECT key FROM $table");
        $str->execute();
        while ( my ($name) = $str->fetchrow_array )
        {
            $known{ $name } += 1;
        }
        $str->finish();
    }

    # The keys we've found
    my @keys = keys %known;

    if ($pattern)
    {
        my @ret;
        foreach my $ent (@keys)
        {
            push( @ret, $ent ) if ( $ent =~ /$pattern/ );
        }
        return (@ret);
    }
    else
    {
        return (@keys);
    }
}


=head2 smembers

Get members of the given set.

=cut

sub smembers
{
    my ( $self, $key ) = (@_);

    if ( !$self->{ 'smembers' } )
    {
        $self->{ 'smembers' } =
          $self->{ 'db' }->prepare("SELECT val FROM sets WHERE key=?");
    }
    $self->{ 'smembers' }->execute($key);

    my @vals;
    while ( my ($name) = $self->{ 'smembers' }->fetchrow_array )
    {
        push( @vals, $name );
    }
    $self->{ 'smembers' }->finish();

    return (@vals);
}


=head2 sismember

Is the given item a member of the set?

=cut

sub sismember
{
    my ( $self, $set, $key ) = (@_);

    my $sql =
      $self->{ 'db' }->prepare("SELECT val FROM sets WHERE key=? AND val=?");
    $sql->execute( $set, $key );

    my $x = $sql->fetchrow_array() || undef;
    $sql->finish();

    if ( defined($x) && ( $x eq $key ) )
    {
        return 1;
    }
    return 0;
}

=head2 sadd

Add a member to a set.

=cut

sub sadd
{
    my ( $self, $key, $val ) = (@_);

    if ( !$self->{ 'sadd' } )
    {
        $self->{ 'sadd' } =
          $self->{ 'db' }->prepare(
            "INSERT INTO sets (key,val) SELECT ?,? WHERE NOT EXISTS( SELECT key, val FROM sets WHERE key=? AND val=? );"
          );

    }
    $self->{ 'sadd' }->execute( $key, $val, $key, $val );
    $self->{ 'sadd' }->finish();
}


=head2 srem

Remove a member from a set.

=cut

sub srem
{
    my ( $self, $key, $val ) = (@_);

    if ( !$self->{ 'srem' } )
    {
        $self->{ 'srem' } =
          $self->{ 'db' }->prepare("DELETE FROM sets WHERE (key=? AND val=?)");
    }
    $self->{ 'srem' }->execute( $key, $val );
    $self->{ 'srem' }->finish();
}


=head2 srandmember

Fetch the value of a random member from a set.

=cut

sub srandmember
{
    my ( $self, $key ) = (@_);

    if ( !$self->{ 'srandommember' } )
    {
        $self->{ 'srandommember' } =
          $self->{ 'db' }->prepare(
                "SELECT val FROM sets where key=? ORDER BY RANDOM() LIMIT 1") or
          die "Failed to prepare";
    }
    $self->{ 'srandommember' }->execute($key);
    my $x = $self->{ 'srandommember' }->fetchrow_array() || "";
    $self->{ 'srandommember' }->finish();

    return ($x);
}


=head2 sunion

Return the values which are present in each of the sets named.

=cut

sub sunion
{
    my ( $self, @keys ) = (@_);


    my @result;

    foreach my $key (@keys)
    {
        my @vals = $self->smembers($key);
        foreach my $val (@vals)
        {
            push( @result, $val );
        }
    }

    return (@result);
}


=head2 sinter

Return only those members who exist in all the named sets.

=cut

sub sinter
{
    my ( $self, @names ) = (@_);

    my %seen;

    foreach my $key (@names)
    {
        my @vals = $self->smembers($key);
        foreach my $val (@vals)
        {
            $seen{ $val } += 1;
        }
    }

    my @result;

    foreach my $key ( CORE::keys(%seen) )
    {
        if ( $seen{ $key } == scalar @names )
        {
            push( @result, $key );
        }
    }
    return (@result);
}



=head2 sinterstore

Return only those members who exist in all the named sets.

=cut

sub sinterstore
{
    my ( $self, $dest, @names ) = (@_);

    # Get the values that intersect
    my @update = $self->sinter(@names);

    # Delete the current contents of the destination.
    $self->del($dest);

    # Now store the members
    foreach my $ent (@update)
    {
        $self->sadd( $dest, $ent );
    }

    # Return the number of entries added
    return ( scalar @update );
}


=head2 scard

Count the members of the given set.

=cut

sub scard
{
    my ( $self, $key ) = (@_);

    if ( !$self->{ 'scard' } )
    {
        $self->{ 'scard' } =
          $self->{ 'db' }->prepare("SELECT COUNT(id) FROM sets where key=?");
    }
    $self->{ 'scard' }->execute($key);
    my $count = $self->{ 'scard' }->fetchrow_array() || 0;
    $self->{ 'scard' }->finish();

    return ($count);
}



sub quit
{
    warn "Method not implemented: quit";
}

sub expire
{
    warn "Method not implemented: expire";
}

1;



=head1 AUTHOR

Steve Kemp

http://www.steve.org.uk/

=cut



=head1 LICENSE

Copyright (c) 2016 by Steve Kemp.  All rights reserved.

This module is free software;
you can redistribute it and/or modify it under
the same terms as Perl itself.
The LICENSE file contains the full text of the license.

=cut
