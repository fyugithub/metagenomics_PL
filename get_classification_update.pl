#!/usr/bin/env perl
use warnings;
use strict;
use Time::HiRes qw(gettimeofday);
###############################################
my $filedir = $ARGV[0]; ####
$filedir =~ s/^\s+//;
$filedir =~ s/\s++$//;

my $outputdir = $ARGV[1];
$outputdir =~ s/^\s+//;
$outputdir =~ s/\s++$//;

my $clafile = $ARGV[2];
$clafile =~ s/^\s+//;
$clafile =~ s/\s++$//;

my $repfile = $ARGV[3];
$repfile =~ s/^\s+//;
$repfile =~ s/\s++$//;

my $unclass ="unclassified";
my $title= "Input_reads \t Classified_taxa\tClassified_reads\tClassified_%\tUnclassified_reads\tUnclassified_%";

my $dataname = $repfile;
my $file1 = "$filedir/$clafile";
my $file2 = "$filedir/$repfile";

my $progfile ="$outputdir/$dataname\_status.txt";
#my $fw2 = openw($progfile);
#print $fw2 "$title\n";

my %taxinfo =get_taxinfo($file2);
my %taxcount =();	####count the apped reads
my %seqcount =();   	####check redundance of reads

$dataname =~ s/\_report\.csv//;

my $otf = "$outputdir/$dataname\_tax-info.txt";
my $fw1=openw($otf);

my $fr=openr($file1);
my $count = 0;
my $countn =0;
my $total=0;
my $line = <$fr>;
while(!eof($fr))
{
        my $line = <$fr>;
       	chomp $line; 
	my @tmp=split /\t/, $line;
	my $rid = $tmp[0];
	my $accid =$tmp[1];
	my $taxid =$tmp[2];
	$seqcount{$rid}++;
	if($accid =~ /$unclass/)
	{
		$countn++;
	}else{
		#	if($taxcount{$taxid})
		#	{
				$taxcount{$taxid}++;
		#	}
		$count++;
	}
	$total++;
	if($total %1000000 ==0)
	{
		print " $total reads processed !\n";
	}
}
        ##################### output results ########
print "done the sequences count: $total\n"; 

my @tseq = sort keys %seqcount;
my $totalseq = @tseq;

delete($taxcount{0});
my @tx = sort keys %taxcount;
my $totaltx = @tx;

print " the toal reads: $total  the taxa count: $totaltx  the unclassified reads: $countn   the classified reads: $count\n";

my $unclapercent =$countn/$total*100;
my $clapercent =$count/$total*100;


print $fw1 "$title\n";
print $fw1 "$totalseq\t$totaltx\t$count\t$clapercent\t$countn\t$unclapercent\n";
#print $fw2 "$totalseq\t$totaltx\t$count\t$clapercent\t$countn\t$unclapercent\n";


print $fw1 "\n";
print $fw1 "Tax_ID\tTax_name\tTax_rank\tMapped_reads\n";
foreach my $tid(@tx)
{
	my $tsz = $taxcount{$tid};
	my $tinfo ="";
	if($taxinfo{$tid})
	{
		$tinfo = $taxinfo{$tid};
	}else
	{
		$tinfo = "\t";
	}
	my $output ="$tid\t$tinfo\t$tsz";
	print $fw1 "$output\n";
}
close($fr);close($fw1);
#close($fw2);
exit;
################################################################
sub getdatafiles
{
       	my $chrfile = shift;
        my @temp =();
        my $chrfw =openr($chrfile);

      	while(!eof($chrfw))
        {
               	my $line1 = <$chrfw>;
                chomp $line1;
                push(@temp, "$line1");
        }
        close($chrfw);

        return @temp;
}

sub get_taxinfo
{
        my $chr = shift;
        my %temp =();
        my $fr1 =openr($chr);
	my $line = <$fr1>;
	my $count=0;
        while(!eof($fr1))
        {
               	$line = <$fr1>;
                chomp $line;
		my @tmp1 =split /\t/, $line;
		my $sze =@tmp1;
		if ($sze >6)
		{
			my $name =$tmp1[0];
			my $taxid=$tmp1[1];
			my $taxrank =$tmp1[2];
                	$temp{$taxid}="$name\t$taxrank";
			$count++;
        	}
	}
	print "The total processed taxa line: $count\n";
        close($fr1);
        return %temp;
}

sub extract_filename
{
    my $infile = shift;
    $infile =~ s/\/$//;
    $infile =~ s/.+\///;
    return $infile;
}


sub openw
{
    my $file = shift;
    my $FW;
    open($FW, ">$file") || die "Couldn't open W $file\n";
    return $FW;
}


sub openr
{
    	my $file = shift;
	my $FR;
	open($FR, "<$file") || die "Couldn't open R $file\n";
	return $FR;
}

