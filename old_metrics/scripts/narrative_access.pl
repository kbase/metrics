#!/usr/bin/perl
#
# This should be run on the narrative system
#

use JSON;
use strict;

my $json = JSON->new->allow_nonref;

my $mons = {'Jan'=>1,'Feb'=>2,'Mar'=>3,'Apr'=>4,'May'=>5,'Jun'=>6, 'Jul'=>7,'Aug'=>8,'Sep'=>9,'Oct'=>10,'Nov'=>11,'Dec'=>12};

my %access;

while (my $line = <STDIN>) {
# Use a regex to parse the log format. This assumes use of the
# combined log format. This is the default for nginx
# Assign values to the same variable names used in "log_format"
# http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format
   my ($remote_addr,
        $remote_user,,
        $time_local,
        $request,
        $status,
        $body_bytes_sent,
        $http_referer,
        $http_user_agent) = ($line =~ /^(\d+\.\d+\.\d+\.\d+)\s-\s(.*?)\s\[(.*?)\]\s\"(.*?)\"\s(\d+?)\s(\d+?)\s\"(.*?)\"\s\"(.*?)\"/);

  next if $status ne 200;
  next if $request=~/\?/;
  my $ws=$request;
  $ws=~s/ HTTP.*//;
  $ws=~s/.*\///;
  my $time=$time_local;
  $time=~s/:.. .*//;
  my $key="$ws $time $remote_addr";
  $access{$key}++;   
 
#205.254.147.8 - - [30/Oct/2014:10:39:27 -0500] "GET /narrative/ws.2177.obj.9 HTTP/1.1" 200 9688 "https://narrative.kbase.us/loading.html?n=http%3a%2f%2fnarrative.kbase.us%2fnarrative%2fws.2177.obj.9" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36"

}

my $summary;
for (sort keys %access){
  my ($ws,$time,$addr)=split;
  my ($date,$hour)=split /:/,$time;
  my ($day,$mon,$year)=split /\//,$date;
  my $mstr=sprintf "%d-%02d",$year,$mons->{$mon};
  my $dstr=sprintf "%d-%02d-%02d",$year,$mons->{$mon},$day;
  if (! defined $summary->{by_workspace}->{$ws}){
    $summary->{by_workspace}->{$ws}->{first_access}=$dstr;
    $summary->{by_month}->{$mstr}->{new_narratives}++;
  }
  $summary->{by_workspace}->{$ws}->{access_count}++;
  $summary->{by_workspace}->{$ws}->{by_ip}->{$addr}++;
  $summary->{by_date}->{$dstr}->{access_count}++;
  $summary->{by_month}->{$mstr}->{access_count}++;
#  print "$_\n";
}

print $json->encode($summary);
