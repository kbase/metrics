#!/usr/bin/env perl
#
# Shane Canon
#
# Reads a CSV file of visits by day (genreated from Splunk)
# and dumps out summaries statistics by month.
#
use JSON;
use strict;

my $json = JSON->new->allow_nonref;

$_=<STDIN>;
my @users=split /,/;
shift @users;

my %staff;

open(E,"kbase-staff.lst") or die "Unable to open KBase Staff list";
while(<E>){
  chomp;
  $staff{$_}=1;
}
close E;

my %new;
my %newbytime;
my %repbytime;
my %totbytime;
my %repeat;
my $users;

my $cts;
$cts->{excludes_internal_kbase}='Y';
$cts->{return_user_window}='> 1 Month';

while(<STDIN>){
  my @list=split /,/;
  my $time=shift @list;
  # Convert to a month
  $time=~s/...T.*//;
  $time=~s/"//;
  my $i=0;
  $cts->{start}=$time unless defined $cts->{start};
  $cts->{end}=$time;
  foreach (@list){
    my $user=$users[$i];
    $i++;
    next if $user eq '"-"';
    next if $user eq 'NULL';
    next if defined $staff{$user};
    next if $_ < 1;
    $users->{$user}->{first}=$time unless defined $users->{$user}->{first};
    if ( ! defined $new{$user}){
      $new{$user}=$time;
      $newbytime{$time}{$user}=1;
      $cts->{cummulative_users}++;
      #$newtime{$time}.=" $user";
      #print "$time new    $user\n";
    }
    elsif ( ( ! defined $repeat{$user} ) && $users->{$user}->{first} ne $time){
      #print "rep: $time repeat $user\n";
      $repeat{$user}=$time;
      $repbytime{$time}{$user}=1;
      #$reptime{$time}.=" $user";
      $cts->{cummulative_return_users}++;
    }
    #$tot{$time}.=" $user" if $_ > 0;
    $totbytime{$time}{$user}=1;
  }
  $cts->{by_month}->{$time}->{cummulative_users}=scalar keys %new;
  $cts->{by_month}->{$time}->{cummulative_return_users}=scalar keys %repeat;
  $cts->{by_month}->{$time}->{new_users}=scalar keys %{$newbytime{$time}};
  $cts->{by_month}->{$time}->{return_users}=scalar keys %{$repbytime{$time}};
  $cts->{by_month}->{$time}->{total_users}=scalar keys %{$totbytime{$time}};
  #$reptime{$time}=$repusers;
}

print $json->encode($cts);
