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

On the command line, type ```slurm.install``` to set up your preferences. 

- **user:** The user name you use to connect to your HPC cluster
- **keyFile:** The SSH private key you use to authenticate. 
- **host:** The IP address or hostname of your cluster
- **remoteStorage:** A folder on the host where files can be saved. This includes files used to start jobs on the cluster and the output of jobs running on the cluster. (E.g., something like /scratch/username/jobStorage/)
- **localStorage:** A folder on the client where files can be saved. This is mainly used for files needed to start jobs. (e.g., /tmp or c:\temp)
- **nodeTempDir:** A folder that exists on all compute nodes. Intermediate job results will be written here. At the end of a job they are written to the headNodeDir. (e.g., /mnt/scratch or /tmp).
- **headRootDir:** A folder that exists on the head node and from where you can retrieve the data. At the end of a job, results are written here.
- **matlabRoot:**  The folder  where the Matlab executable lives on the cluster. If specified, mslurm starts matlab by executing the MatlabRoot/matlab command on the cluster. (If left empty, this only works if 'matlab' is on your path on the cluster.)

These preferences are unlikely to change frequently, but you can always update them by calling ```slurm.install``` again, or ```slurm.setpref```. 

## Usage
Once the preferences are set, you create a new mslurm object by calling the constructor:
```matlab
c=mslurm;
```
Each of these parameters can also be changed for the duration of a session by passing arguments to the constructor (see ```help slurm/slurm```).

The jobs posted to SLURM in each session typically share several parameters. You set these by assigning values to the mslurm object. 

- **startupDirectory:** The folder where Matlab will start (this could be a place where your startup.m and pathdef.m live).
- **workingDirectory:** The folder where the jobs will execute. If left unspecified (''), this will use a unique subfolder of the remoteStorage folder created for the job.
- **addPath:** A string/char specifying the path that should be included on the cluster. This string will be passed to addpath() on the cluster.
- **batchOptions:** A cell array of parameter/value pairs that are passed verbatim to the slurm sbatch command. See the (man page for sbatch for all options)[https://slurm.schedmd.com/sbatch.html]. These options are key to request the right kind of compute nodes for your job.
- **runOptions:** A string/char that is passed verbatim to the ```srun``` command. You probably do not need this, but see the (man page)[https://slurm.schedmd.com/srun.html] for valid options.

For example:
```matlab
c.addPath = '/home/bart/mslurm'; % Making sure that the mslurm path is accessible on the cluster
c.batchOptions = {'mem','32GB','time',30}; %Request workers with at least 32GB of memory and a wall time of 30 minutes. 
```




## Usage
See slurmExample.m in the mslurm directory for a commented example how to use the toolbox.

The slurm.sbatch function is the low-level workhorse that cant submit any Matlab job to SLURM. But to make this useful you will want to specify what each of those jobs have to do. For simple jobs you could directly pass arguments to sbatch, but for anything more complicated you will probably want to write some files that contain parameters for each of the jobs. The slurm.feval and slurm.fileInFileOut are two example workflows; use them as is or modify them to match your workflow.


[![DOI](https://zenodo.org/badge/93510696.svg)](https://zenodo.org/badge/latestdoi/93510696)
