# mslurm

This is a toolbox to send Matlab jobs from your local machine (the client) to an HPC cluster running the SLURM scheduler (the server). As long as you have unlimited licenses to run Matlab on the cluster, this allows you to run many jobs in parallel, without the distributed computing server license. This only works for "dumb" parallelism though; messaging between jobs is not used. A simple app shows information from the slurm acccounting log on the server, can retrieve data and log files, and allows you to restart failed jobs.


*** Note that this toolbox does not use the Matlab Parallel Server. The disadvantage is that its many nice/useful features are not accessible. The advantage is that you don't need a MPS license. If you have an MPS license, then [kSlurm](https://github.com/klabhub/kSlurm) may be a better option for you. Although you should be warned that debugging code that runs through MPS is not for the faint of heart. ***

## Installation:

### Server side

Clone the mslurm repository to the server.
Add the mslurm directory to the Matlab search path on the server.

### Client Side
Clone the mslurm repository to the client.
Add the mslurm directory to the Matlab search path on the client.

On the command line, type
slurm.install 
to setup your preferences 
USER: The 

## Usage
See slurmExample.m in the mslurm directory for a commented example how to use the toolbox.

The slurm.sbatch function is the low-level workhorse that cant submit any Matlab job to SLURM. But to make this useful you will want to specify what each of those jobs have to do. For simple jobs you could directly pass arguments to sbatch, but for anything more complicated you will probably want to write some files that contain parameters for each of the jobs. The slurm.feval and slurm.fileInFileOut are two example workflows; use them as is or modify them to match your workflow.


[![DOI](https://zenodo.org/badge/93510696.svg)](https://zenodo.org/badge/latestdoi/93510696)
