#!/usr/bin/env perl
#
# Shane Canon
#
# Reads a CSV file of visits by day (genreated from Splunk)
# and dumps out summaries statistics by month.
#
# TODO: Convert to JSON format

$_=<STDIN>;
@users=split /,/;
shift @users;

open(E,"kbase-staff.lst") or die "Unable to open KBase Staff list";
while(<E>){
  chomp;
  $staff{$_}=1;
}
close E;

while(<STDIN>){
  @list=split /,/;
  $time=shift @list;
  $time=~s/...T.*//;
  $time=~s/"//;
  $i=0;
  $newusers=0;
  $repusers=0;
  foreach (@list){
    $user=$users[$i];
    $i++;
    next if $user eq '"-"';
    next if $user eq 'NULL';
    next if defined $staff{$user};
    next if $_ eq 0;
    if ( ! defined $new{$user}){
      $new{$user}=$time;
      $newbytime{$time}{$user}=1;
      $cumnew++;
      $newusers++;
      #$newtime{$time}.=" $user";
      #print "$time new    $user\n";
    }
    elsif ( ! defined $repeat{$user}){
      #print "$time repeat $user\n";
      $repeat{$user}=$time;
      $repbytime{$time}{$user}=1;
      #$reptime{$time}.=" $user";
      $repusers++;
      $cumrep++;
    }
    #$tot{$time}.=" $user" if $_ > 0;
    $totbytime{$time}{$user}=1;
    $tot{$time}++;
  }
  $cum{$time}=scalar keys %new;
  $cumrep{$time}=scalar keys %repeat;
  $newtime{$time}=scalar keys %{$newbytime{$time}};
  $reptime{$time}=scalar keys %{$repbytime{$time}};
  $tot{$time}=scalar keys %{$totbytime{$time}};
  #$reptime{$time}=$repusers;
}

print "Date,New,Ret,Users,Cummulative,CumRep\n";
foreach my $t (sort keys %newtime){
  #$reps=scalar split / /,$reptime{$t};
  #printf "%s %d %s\n",$t,scalar @l-1, $newtime{$t};
  #printf "%s %4d %4d %4d\n",$t,$new,$reps,$tot;
  printf "%s,%4d,%4d,%4d,%4d,%4d\n",$t,$newtime{$t},$reptime{$t},$tot{$t},$cum{$t},$cumrep{$t};
}
