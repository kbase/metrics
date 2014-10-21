#!/usr/bin/perl -w
# based on the web page:
#   http://blogs.splunk.com/2011/08/02/splunk-rest-api-is-easy-to-use/
# by F.K. 
# Modified by Masa@Splunk
# Modified by scanon@lbl.gov
#
# Help script to fetch output from a session.  Great for long running queries.
#

# modules
use strict;
use Data::Dumper;
$Data::Dumper::Indent=1;
use LWP::UserAgent;  # Module for https calls
use XML::Simple;     # convrt xml to hash
use URI::Escape;     # sanitize searches to web friendly characters

$ENV{'PERL_LWP_SSL_VERIFY_HOSTNAME'}=0;

# init environment 
my $base_url = 'https://kbase.us/';
my $username = 'admin';
my $password = $ENV{'SPLUNKPW'};
my $app      = 'search';

my $XML = new XML::Simple;
my $ua = LWP::UserAgent -> new;

my $post;         # Return object for web call
my $results;      # raw results from Splunk
my $xml;          # pointer to xml hash


# Request a session Key 
$post = $ua->post(
         "$base_url/servicesNS/admin/$app/auth/login",
         Content => "username=$username&password=$password"
      );
$results = $post->content;
$xml = $XML->XMLin($results);

# Extract a session key
my $ssid = "Splunk ".$xml->{sessionKey};
print STDERR "Session_Key(Authorization): $ssid\n";

# Add session key to header for all future calls
$ua->default_header( 'Authorization' => $ssid);



# Get Search ID
my $sid = $ARGV[0];
print STDERR  "SID(Search ID)            : $sid\n";


# Check the search Status
# Repeat until isDone is 1
#   <s:key name="isDone">1</s:key>
my $done;
my $prog;
do {
   sleep(2);
   $post = $ua->get(
            "$base_url/servicesNS/$username/$app/search/jobs/$sid/"
         );
   $results = $post->content;
   if ( $results =~ /name="doneProgress">([^<]*)</ ) {
      $prog = $1*100;
   } else {
      $prog = '-';
   }
   $results = $post->content;
   if ( $results =~ /name="isDone">([^<]*)</ ) {
      $done = $1;
   } else {
      $done = '-';
   }
   print STDERR "Progress Status:$done: $prog Running\n";
} until ($done eq "1");


# Get Search Results
$post = $ua->get(
         "$base_url/servicesNS/$username/$app/search/jobs/$sid/results?output_mode=csv&count=0"
      );
$results = $post->content;
#$xml = $XML->XMLin($results);
print "$results";
print STDERR "\nSearch is completed.\n\n";
