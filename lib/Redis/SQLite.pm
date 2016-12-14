
=head1 NAME

Redis::SQLite - Redis API-compatible storage system using SQLite.

=cut

=head1 DESCRIPTION

This package is an implementation of the L<Redis> Perl-client API, which
stores all data in an SQLite database.

It is I<not> a drop-in replacement, because it doesn't implement all the
features you'd expect from the real Redis module.  Just enough to be useful.

=cut

package Redis::SQLite;



use strict;
use warnings;
use DBI;


our $VERSION = '0.1';


=begin doc

Constructor

=end doc

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


=begin doc

Get the value of a string-key.

=end doc

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



=begin doc

Set the value of a string-key.

=end doc

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



=begin doc

Increment and return the value of an (integer) string-key.

=end doc

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



=begin doc

Decrement and return the value of an (integer) string-key.

=end doc

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



=begin doc

Delete a given key, regardless of whether it holds a string or a set.

=end doc

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


=begin doc

Get known-keys.  These can be optionally filtered by a (perl) regular
expression.

=end doc

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


=begin doc

Get members of the given set.

=end doc

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


=begin doc

Is the given item a member of the set?

=end doc

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

=begin doc

Add a member to a set.

=end doc

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


=begin doc

Remove a member from a set.

=end doc

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


=begin doc

Fetch the value of a random member from a set.

=end doc

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


=begin doc

Return the values which are present in each of the sets named.

=end doc

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


=begin doc

Return only those members who exist in all the named sets.

=end doc

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


=begin doc

Count the members of the given set.

=end doc

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

#
#  End of the module.
#
1;
