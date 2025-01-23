#!/usr/bin/env perl

use warnings;
use strict;
use Time::HiRes qw(gettimeofday);
#########################
my $filedir = $ARGV[0];			####### the directory of the samples
$filedir =~ s/^\s+//;
$filedir =~ s/\s++$//;
 
my $outputdir = $ARGV[1];		#######  The output directory
$outputdir =~ s/^\s+//;
$outputdir =~ s/\s++$//;

my $seqlist = $ARGV[2];			####### The list of samples to be evaluated
$seqlist =~ s/^\s+//;
$seqlist =~ s/\s++$//;
#############################################
my $pbsdir = createpbsdir($outputdir);
my @datafiles = getseqfiles($seqlist);

foreach my $dataset (@datafiles)
{
	chomp $dataset;
	my @tmp = split /\t/, $dataset;
	my $file1 = $tmp[0];
	my $file2 = $tmp[1];
        my $dataname = $file2;
        $dataname =~ s/\_report\.csv//;
        
	my $cmd1  = "get_classification_update.pl $filedir $outputdir $file1 $file2";

        my $jobname     = getjobname();

        my $pbsfile ="$pbsdir/$dataname\.sh";
        my $errfile ="$pbsdir/$dataname\.err";
        my $outfile ="$pbsdir/$dataname\.out";
        makepbsfile($pbsfile, $cmd1, $jobname, $errfile, $outfile);
 
}


my @pbsfiles = getpbsfiles($pbsdir);

foreach my $cmd1 (@pbsfiles)
{
        my $jobid = submitjob($cmd1);
        print "$jobid\n";
}

exit;

#################################################

sub getseqfiles		######## the list of the output from centrifuge: the classification.txt and report.csv ##############
{
 	my $chrfile = shift;
        my @temp =();
        my $chrfw =openr($chrfile);

         while(!eof($chrfw))
        {
                my $line1 = <$chrfw>;
                chomp $line1;
		my $line2 = <$chrfw>;
                chomp $line2;

                push(@temp, "$line1\t$line2");
        }
        close($chrfw);

        return @temp;
}


sub getpbsfiles
{
    my $dir  = shift;
    my @temp = ();
 
    if(-e $dir){;}
        else{print "no reads file at this time\n"; return @temp;}
 
    opendir(DIR1,$dir) || die "Couldn't open this directory\n";
 
    while(my $file = readdir(DIR1))
    {
      if($file =~ /.sh$/)
        {
       push(@temp, "$dir/$file");
        }
    }closedir(DIR1);
 
    return @temp;
}

sub extract_filename
{
    my $infile = shift;
    $infile =~ s/\/$//;
    $infile =~ s/.+\///;
    return $infile;
}


sub makepbsfile
{
    	my $pbsfile = $_[0];
    	my $cmd1    = $_[1];
    	my $jobname = $_[2];
	my $errfile = $_[3];
 	my $outfile = $_[4];

    	my $pbstemplate = getpbstemplate(); #print "$pbstemplate\n";
    	my @tmp         = split /\n/, $pbstemplate;
    	my $fw;
	open($fw, ">$pbsfile") || die "Could not open $pbsfile\n"; 
	foreach my $line (@tmp)
        {
        	if ($line =~ /(\#SBATCH -J )(icbrfyu)/)
                {
                      	$line = "$1$jobname\n";print $fw $line;
                }
		elsif ($line =~ /(\#SBATCH -e )(test.err)/)
                {
               		$line = "$1$errfile\n";print $fw $line;
                }
		elsif ($line =~ /(\#SBATCH -o )(test.out)/)
                {
                        $line = "$1$outfile\n";print $fw $line;
                }
		else
		{
			print $fw "$line\n";
		}
	}

	print $fw "$cmd1\n";
	close($fw);
}

##################################### the following commands should be changed based on the SLUM workload manager (SLURM) ##############
sub getpbstemplate
{
    	my $tmp = "";
 
       	$tmp .= "\#! \/bin\/bash\n";
	#$tmp .= "\#SBATCH --qos=XXXXXXX\n";
	$tmp .= "\#SBATCH -J XXXXXX\n";  	###name of job 
	$tmp .= "\#SBATCH -e test.err\n";        ###standard error
	$tmp .= "\#SBATCH -o test.out\n";        ###standard output       
	$tmp .= "\#SBATCH --nodes=1\n"; 
 	$tmp .= "\#SBATCH --tasks=1\n";  
        $tmp .= "\#SBATCH --cpus-per-task=14\n"; 
	$tmp .= "\#SBATCH --mem=120gb\n";       #### ram for each node
       	$tmp .= "\#SBATCH -t 96:00:00\n";		###walltime
 
       return $tmp;
}

sub createpbsdir
{
    	my $pbsdir  = shift; 
	$pbsdir = "$pbsdir/pbs";
       	if( -e $pbsdir){;} else{system("mkdir $pbsdir");}
    	return $pbsdir;
}


sub submitjob
{
   	my $pbsfile   = shift;
    	my $cmd       = "sbatch $pbsfile"; print "$cmd\n";
       
    	open(CMD, "$cmd |") || die "Can't run $cmd\n";
 	my $jobid = <CMD>;close(CMD);
 	chomp $jobid;
 
       	$jobid =~ s/^(\d+)\..+/$1/;
 
       	return $jobid;
}


sub getjobname
{
    my ($seconds, $microseconds) = gettimeofday;
    my $jobname     = $seconds;
       return $jobname;
}

sub openr
{
        my $file = shift;
        my $FR;
        open($FR, "<$file") || die "Couldn't open R $file\n";
            return $FR;
 }

