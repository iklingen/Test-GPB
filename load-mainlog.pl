#!/usr/bin/env perl
use strict;
use Getopt::Long;
use DBI qw(:sql_types);

use constant DB_SERVICE => 'IT1';
sub db_store(@);
sub db_disconnect();

use constant VALID_FLAG => {
    '<=' => 1,
    '=>' => 1,
    '->' => 1,
    '**' => 1,
    '==' => 1,
};

my %Opt = (
    'print-stats'  => 1,
    'print-emails' => 0,
    'warn-flag'    => 0,
    'warn-email'   => 0,
    'warn-id'      => 0,
);
GetOptions( \%Opt, 'print-stats|stats|s!', 'print-emails|emails|e!',
    'warn-flag|wf!', 'warn-email|we!', 'warn-id|wi!' );

my ( $line_count, $record_count, $empty_id_count, $wrong_count, %ADDRESS,
    %INT_ID, %FLAG );

my %counts;

while (<>) {
    chomp;

    my $line = $_;
    $counts{'lines'} += 1;

    my ( $date,   $time, $str )   = split / /, $line, 3;
    my ( $int_id, $flag, $other ) = split / /, $str,  3;

    if ( $int_id =~ /Start|End/i || $flag eq 'Completed' ) {
        $counts{'unessential'} += 1;
        next;
    }

    unless ( VALID_FLAG->{$flag} ) {
        warn "F $line\n" if $Opt{'warn-flag'};
        $counts{'wrong-flag'} += 1;
        next;
    }

    # extract raw email address with maybe followed ' foo=' and skip lines with a wrong email
    unless ( $other =~ m/^(.+?@.+?(=|$))/ ) {
        warn "E $line\n" if $Opt{'warn-email'};
        $counts{'wrong-email'} += 1;
        next;
    }

    my $address = $1;

    # remove possible tail ' foo='
    $address =~ s/\s\S*=$//;

    # clear email: 'comment <email@address>' -> 'email@address'
    if ( $address =~ m/<(\S+?@\S+?)>/ ) {
        $address = $1;
    }

    # special case: 'foo@email.bar: some text'
    $address =~ s/:.*$//;

    # extract id
    $str =~ m/(?:^|\s|)id=(\S+)/;
    my $id = $1;

    # 'id=' is mandatory only for '<=' records
    unless ( $id || $flag ne '<=' ) {
        warn "I $line\n" if $Opt{'warn-id'};
        $counts{'wrong-id'} += 1;
        next;
    }

    $counts{'parsed'} += 1;

    $ADDRESS{$address}{$flag} += 1;
    $INT_ID{$int_id}          += 1;
    $FLAG{$flag}              += 1;

    if ( db_store( $date, $time, $int_id, $id, $flag, $address, $str ) ) {
        $counts{'processed'} += 1;
    }
    else {
        $counts{'wrong-db'} += 1;
        warn "*** Line number $counts{'lines'}\n*** $line\n\n";
    }
}

db_disconnect;

my $max_chain_count = 0;
for ( values %INT_ID ) {
    $max_chain_count = $_ if $_ > $max_chain_count;
}

for (
    qw(lines parsed processed unessential wrong-flag wrong-email wrong-id wrong-db )
  )
{
    $counts{$_} += 0;
}

if ( $Opt{'print-stats'} ) {
    print "Lines read            $counts{'lines'}\n";
    print "Records parsed        $counts{'parsed'}\n";
    print "Records processed     $counts{'processed'}\n\n";
    print "Unessential skipped   $counts{'unessential'}\n";
    print "Wrong flag  rejected  $counts{'wrong-flag'}\n";
    print "Wrong email rejected  $counts{'wrong-email'}\n";
    print "Empty id    rejected  $counts{'wrong-id'}\n";
    print "DB error    rejected  $counts{'wrong-db'}\n\n";
    print "Unique emails count   ", scalar keys %ADDRESS, "\n";
    print "Unique chains count   ", scalar keys %INT_ID,  "\n";
    print "Max chain count       $max_chain_count\n";

    print "Flags counts:\n";
    for ( sort keys %FLAG ) { print "  '$_' : $FLAG{$_}\n" }
}

if ( $Opt{'print-emails'} ) {
    print "Emails:\n";
    for my $addr ( sort keys %ADDRESS ) {
        print "  $addr\t => {",
          join( ', ',
            map { "'$_' : $ADDRESS{$addr}{$_}" }
            sort keys %{ $ADDRESS{$addr} } ),
          "}\n";
    }
}

exit;

########################################################################

{
    my $db;
    my %st;
    my $skip;
    my $error;

    sub db_open($) {
        my $service = shift;
        eval {
            $db = DBI->connect( "dbi:Pg:service=$service", '', '',
                { AutoCommit => 0, PrintError => 1, RaiseError => 1 } );
            $db->do('delete from message');
            $db->do('delete from log');
            $st{mes} = $db->prepare(
                'insert into message(created, int_id, id, str) values(?,?,?,?)'
            );
            $st{log} = $db->prepare(
                'insert into log(created, int_id, address, str) values(?,?,?,?)'
            );
        };
        if ($@) {

            # do not insert data
            $skip  = 1;
            $error = 1;
        }
    }

    sub db_store(@) {
        return 0 if $skip;

        my ( $date, $time, $int_id, $id, $flag, $address, $str ) = @_;
        db_open(DB_SERVICE) unless $db;

        eval {
            if ( $flag eq '<=' ) {
                $st{mes}->bind_param( 1, "$date $time", SQL_TIMESTAMP );
                $st{mes}->bind_param( 2, $int_id,       SQL_VARCHAR );
                $st{mes}->bind_param( 3, $id,           SQL_VARCHAR );
                $st{mes}->bind_param( 4, $str,          SQL_VARCHAR );
                $st{mes}->execute;
            }
            else {
                $st{log}->bind_param( 1, "$date $time", SQL_TIMESTAMP );
                $st{log}->bind_param( 2, $int_id,       SQL_VARCHAR );
                $st{log}->bind_param( 3, $address,      SQL_VARCHAR );
                $st{log}->bind_param( 4, $str,          SQL_VARCHAR );
                $st{log}->execute;
            }
        };
        if ($@) {
            $error = 1;
            return 0;
        }
        return 1;
    }

    sub db_disconnect() {
        if ($db) {
            if ($error) {
                $db->rollback;
            }
            else {
                $db->commit;
            }
            $db = undef;
        }
    }

}
