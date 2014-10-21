#!/usr/bin/perl -w
# based on the web page:
#   http://blogs.splunk.com/2011/08/02/splunk-rest-api-is-easy-to-use/
# by F.K. 
# Modified by Masa@Splunk
#
# This gets the users who accessed worksapce by day in CSV format.
#

# modules
use strict;
use Data::Dumper;
$Data::Dumper::Indent=1;
use LWP::UserAgent;  # Module for https calls
use XML::Simple;     # convrt xml to hash
use URI::Escape;     # sanitize searches to web friendly characters

$ENV{'PERL_LWP_SSL_VERIFY_HOSTNAME'}=0;

# Searche
#  Note: be careful with quota and special characters
my $SEARCH;
$SEARCH ='
search workspace | timechart span=1d count by user useother=f limit=1000
';

# If we want to call a saved search
# $SEARCH = '|savedsearch "DasDnsDQ"';


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

# Perform a search
$post = $ua->post(
         "$base_url/servicesNS/$username/$app/search/jobs", 
         Content => "search=".uri_escape($SEARCH)
      );
$results = $post->content;
$xml = $XML->XMLin($results);

# Check for valid search
unless (defined($xml->{sid})) {
   print STDERR "Unable to run command\n$results\n";
   exit;
}

# Get Search ID
my $sid = $xml->{sid};
print STDERR  "SID(Search ID)            : $sid\n";


# Check the search Status
# Repeat until isDone is 1
#   <s:key name="isDone">1</s:key>
my $done;
do {
   sleep(2);
   $post = $ua->get(
            "$base_url/servicesNS/$username/$app/search/jobs/$sid/"
         );
   $results = $post->content;
   if ( $results =~ /name="isDone">([^<]*)</ ) {
      $done = $1;
   } else {
      $done = '-';
   }
   print STDERR "Progress Status:$done: Running\n";
} until ($done eq "1");


# Get Search Results
$post = $ua->get(
         "$base_url/servicesNS/$username/$app/search/jobs/$sid/results?output_mode=csv&count=0"
      );
$results = $post->content;
#$xml = $XML->XMLin($results);
print "$results";
print STDERR "\nSearch is completed.\n\n";
