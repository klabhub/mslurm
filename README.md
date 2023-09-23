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
to setup your preferences:

- **User:** The user name you use to connect to your HPC cluster
- **KeyFile:** The SSH private key you use to authenticate. 
- **Host:** The IP address or hostname of your cluster
- **RemoteStorage:** A folder on the host where files can be saved. This includes files used to start jobs on the cluster and the output of jobs running on the cluster. (E.g., something like /scratch/username/jobStorage/)
- **LocalStorage:** A folder on the client where files can be saved. This is mainly used for files needed to start jobs. (e.g., /tmp or c:\temp)
- **NodeTempDir:** A folder that exists on all compute nodes. Intermediate job results will be written here. At the end of a job they are written to the headNodeDir. (e.g., /mnt/scratch or /tmp).
- **HeadRootDir:** A folder that exists on the head node and from where you can retrieve the data. At the end of a job, results are written here.
- **MatlabRoot:**  The folder  where the Matlab executable lives on the cluster. If specified, mslurm starts matlab by executing the MatlabRoot/matlab command on the cluster. (If left empty, this only works if 'matlab' is on your path on the cluster.)

## Usage
See slurmExample.m in the mslurm directory for a commented example how to use the toolbox.

The slurm.sbatch function is the low-level workhorse that cant submit any Matlab job to SLURM. But to make this useful you will want to specify what each of those jobs have to do. For simple jobs you could directly pass arguments to sbatch, but for anything more complicated you will probably want to write some files that contain parameters for each of the jobs. The slurm.feval and slurm.fileInFileOut are two example workflows; use them as is or modify them to match your workflow.


[![DOI](https://zenodo.org/badge/93510696.svg)](https://zenodo.org/badge/latestdoi/93510696)
