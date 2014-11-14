#!/usr/bin/env perl
#
# Shane Canon
#
# Reads a CSV file of visits by day (genreated from Splunk)
# and dumps out summaries statistics by month.
#
use LWP::Simple;                # From CPAN
use JSON;
use POSIX qw/strftime/;
use strict;
use Data::Dumper;               # Perl core module
use warnings;                   # Good practice

my $outdir=$ARGV[0];

my $json = JSON->new->allow_nonref;

my $accessurl = "https://narrative.kbase.us/access_log/access.json";
my $objurl = "http://metrics.kbase.us/ws_object_list.json";

my $jsontmp = get( $accessurl );
die "Could not get $accessurl!" unless defined $jsontmp;

open(O,"> $outdir/narrative_access.json") or die "Unable to open output ($outdir)";
print O $jsontmp;
close O;

# Decode the entire JSON
my $access = JSON::decode_json( $jsontmp );

$jsontmp = get( $objurl );
die "Could not get $objurl!" unless defined $jsontmp;

# Decode the entire JSON
my $objlist = JSON::decode_json( $jsontmp );

for my $obj (keys %{$access->{'by_workspace'}}){
  next unless defined $objlist->{$obj};
  $objlist->{$obj}->{access_count}=$access->{'by_workspace'}->{$obj}->{access_count};
  $objlist->{$obj}->{by_ip}=$access->{'by_workspace'}->{$obj}->{by_ip};
  $objlist->{$obj}->{first_access}=$access->{'by_workspace'}->{$obj}->{first_access};
  # Some narratives may have been created on narrative-dev
  my ($d,$t)=split /T/,$objlist->{$obj}->{savedate};
  if ($objlist->{$obj}->{first_access} gt $d){
    $objlist->{$obj}->{first_access} = $d;
  }
}

for my $obj (keys %{$objlist}){
  if (! defined $objlist->{$obj}->{access_count}){
    $objlist->{$obj}->{access_count}=0;
    my ($d,$t)=split /T/,$objlist->{$obj}->{savedate};
    $objlist->{$obj}->{first_access}=$d;
  }
  if (! defined $objlist->{$obj}->{name}){
    delete $objlist->{$obj};
    next;
  }
  if ($objlist->{$obj}->{del} eq 'true'){
    delete $objlist->{$obj};
    next;
  }
  delete $objlist->{$obj} if $objlist->{$obj}->{name}=~/^auto[0-9]+/;
}

open(O,"> $outdir/narratives.json") or die "Unable to open output ($outdir)";
print O $json->pretty->encode( $objlist );
close O;
