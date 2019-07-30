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
my $staff_file="kbase-staff.lst";
my $merged_file="narratives2.json";

my $json = JSON->new->allow_nonref;

# Read in the Staff list
#
my %staff;
open(E,$staff_file) or die "Unable to open KBase Staff list";
while(<E>){
  chomp;
  $staff{$_}=1;
}
close E;

# From access logs
my $accessurl = "https://narrative.kbase.us/access_log/access.json";

# From workspace
my $objurl = "http://metrics.kbase.us/ws_object_list.json";

my $jsontmp = get( $accessurl );
die "Could not get $accessurl!" unless defined $jsontmp;

# Write out the access data unaltered
#
open(O,"> $outdir/narrative_access.json") or die "Unable to open output ($outdir)";
print O $jsontmp;
close O;

# Decode the entire JSON
my $access = JSON::decode_json( $jsontmp );

$jsontmp = get( $objurl );
die "Could not get $objurl!" unless defined $jsontmp;

# Decode the entire JSON
my $by_month;
my $objlist = JSON::decode_json( $jsontmp );

# Go through the access logs and augment the workspace data
#
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

# Cleanup and counters
# - Set access to 0 if no access records
# - Remove objects that lack a name
# - Remove objects that are deleted the workspace
# - Remove autosave naratives
 
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

# Update counters for new narratives
#
for my $obj (keys %{$objlist}){
  my $month=substr($objlist->{$obj}->{first_access},0,7);
  $by_month->{$month}->{'new_narrative'}++;
  my $o=$objlist->{$obj}->{savedby};
  my $s=':user';
  $s=':staff' if defined $staff{$o};
  $by_month->{$month}->{'new_narrative'.$s}++;
}


# Generate cumulative values
#
my $l='new_narrative';
my %cum;
for my $t ('',':staff',':user'){ $cum{$l.$t}=0; }
for my $month (sort keys %{$by_month}){
    $by_month->{$month}->{access_count}=$access->{'by_month'}->{$month}->{access_count};
    for my $t ('',':staff',':user'){
       my $v=$by_month->{$month}->{$l.$t};
       $cum{$l.$t}+=$by_month->{$month}->{$l.$t} if defined $v;
       $by_month->{$month}->{'cumulative_narrative'.$t}=$cum{$l.$t};
    }
}

# Assemble object for JSON output
#
my $jout->{'by_month'}=$by_month;
$jout->{'by_workspace'}=$objlist;
$jout->{'by_date'}=$access->{'by_date'};

# Create metadata block
my $date=strftime('%Y-%m-%d',gmtime);
$jout->{meta}->{comments}="Generated from narrative access logs and workspace objects";
$jout->{meta}->{author}="Shane Canon";
$jout->{meta}->{generated}=$date;
$jout->{meta}->{dataset}= "nginx access logs and workspace objects";
$jout->{meta}->{comments}="This merges the narrative access logs from nginx with a dump of the narrative objects in the workspace.\n";

open(O,"> $outdir/$merged_file") or die "Unable to open output ($outdir)";
print O $json->pretty->canonical->encode( $jout );
close O;
