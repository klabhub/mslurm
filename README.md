# mslurm

This is a toolbox to send Matlab jobs from your local machine (the client) to an HPC cluster running the SLURM scheduler (the server). As long as you have unlimited licenses to run Matlab on the cluster, this allows you to run many jobs in parallel, without the distributed computing server license. This only works for "dumb" parallelism though; messaging between jobs is not used. A simple gui shows information from the slumr acccounting log on the server, can retrieve data and log files, and allows you to restart failed jobs.


## Installation:

### Server side
Make sure the command 'matlab' on the HPC cluster's command line starts Matlab. If it doesn't, add the path to  .bashrc. For instance, by adding the following line:
PATH=$PATH:/opt/sw/packages/MATLAB/R2016a/bin/: 
(but of course your installation path for Matlab on the cluster can vary).

Clone the mslurm repository to the server.
Add the mslurm directory to the Matlab search path on the server

### Client Side
Clone the mslurm repository to the client .
Add the mslurm directory to the Matlab search path on the client.

This package depends on the SSH2 package (David Freedman); install it:
Clone the SSH2 repository (https://github.com/davidfreedman/matlab-ssh2.git) to the client and follow its instructions to set it up. 

The GUI depends on the findjobj tool (Yair Altman). Download it from the file exchange:
https://nl.mathworks.com/matlabcentral/fileexchange/14317-findjobj-find-java-handles-of-matlab-graphic-objects
and add it to your Matlab install on the client. 

## Usage
See slurmExample.m in the mslurm directory for a commented example how to use the toolbox.

The slurm.sbatch function is the low-level workhorse that cant submit any Matlab job to SLURM. But to make this useful you will want to specify what each of those jobs have to do. For simple jobs you could directly pass arguments to sbatch, but for anything more complicated you will probably want to write some files that contain parameters for each of the jobs. The slurm.feval and slurm.fileInFileOut are two example workflows; use them as is or modify them to match your workflow.


[![DOI](https://zenodo.org/badge/93510696.svg)](https://zenodo.org/badge/latestdoi/93510696)
