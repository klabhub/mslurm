# mslurm Matlab Toolbox

*Bart Krekelberg - 2015 / June 2017 - Public release / Nov 2023 - Major revision. Release V2.0.*

This Matlab toolbox sends jobs from your local machine (the client) to an HPC cluster running the SLURM scheduler (the server). As long as you have unlimited licenses to run Matlab on the cluster, this allows you to run many jobs in parallel, without the Matlab Compute Server (aka distributed computing server) license. This only works for "dumb" parallelism though; messaging between jobs is not used. A simple app shows information from the slurm acccounting log on the server, can retrieve data and log files, and allows you to restart failed jobs.

***This toolbox does not use the Matlab Compute Server. The [kSlurm](https://github.com/klabhub/kSlurm) toolbox does use MCS, but, in my experience, the features of MCS are not worth the complexity of using it (unexplained Matlab segfault crashes, missing diary files making it impossible to find bugs, etc.).***

## Installation

Here are the necessary steps to get this toolbox to work. The examples use the Rutgers Amarel cluster but should work on any HPC cluster with the SLURM scheduler.

### Authentication  setup

An RSA key allows your local computer to set up a connection with the cluster without a password.  The mslurm toolbox uses this to send commands from the client to the server.
Type the following command on a UNIX or Windows terminal on the client (On Windows, use the PowerShell terminal, not the command prompt.):

```shell
ssh-keygen -t rsa -b 4096 -m PEM
```

Accept the default location for the id_rsa file. Then, send your newly created key to the cluster. `NETID` should be replaced with your login name on the cluster.

 ```shell
cat ~/.ssh/id_rsa.pub | ssh NETID@amareln.hpc.rutgers.edu 'cat >> .ssh/authorized_keys'
ssh NETID@amareln.hpc.rutgers.edu "chmod 700 .ssh; chmod 640 .ssh/authorized_keys"
```

### Server-side install

Clone the mslurm repository on  the server.

```shell
ssh NETID@amareln.hpc.rutgers.edu "git clone https://github.com/klabhub/mslurm"
```

Remember the path where the toolbox was installed. The command above puts it in  `~/mslurm`.

### Client-side install

Clone the mslurm repository to the client.

```shell
git clone https://github.com/klabhub/mslurm
```

Remember the path (e.g., `c:\github\mslurm`) where the toolbox was cloned. Add that folder to the Matlab search path on the client. Use 'Set Path' in the Matlab IDE or:

```matlab
addpath('c:\github\mslurm')
savepath
```

On the Matlab command line, type ```slurm.install``` to set up your preferences.
This will ask for the following information:

- **user:** The user name you use to connect to your HPC cluster (In the example; this would be NETID)
- **keyFile:** The SSH private key you use to authenticate.   (In the example: `~.ssh/id_rsa`)
- **host:** The IP address or hostname of your cluster. (In the example: amareln.hpc.rutgers.edu)
- **remoteStorage:** A folder on the host where files can be saved. This includes files used to start jobs on the cluster and the output of jobs running on the cluster. (E.g., something like `/scratch/<NETID>/mslurmStorage/`)
- **localStorage:** A folder on the client where files can be saved. This is mainly used for files needed to start jobs. (e.g., `/tmp/mslurmStorage` or `c:\temp\mslurmStorage`)
- **nodeTempDir:** A folder that exists on all compute nodes. Intermediate job results will be written here. At the end of a job they are written to the remoteStorage.  (e.g., `/mnt/scratch` or `/tmp`).
- **matlabRoot:**  The folder  where the Matlab executable lives on the cluster. If specified, mslurm starts matlab by executing the MatlabRoot/matlab command on the cluster. (If left empty, this only works if `matlab` is on your path on the cluster.). On the Amarel cluster this is `/opt/sw/packages/MATLAB/R2023a/bin` for the R2023a release. Other releases are available too.
- **mslurmFolder** The folder where the mslurm package is installed on the cluster. In the example above, this is `~/mslurm`

These preferences are unlikely to change frequently, but you can always update them by calling ```slurm.install``` again, or ```slurm.setpref``` to change one of these preferences. Each of these parameters can also be changed for the duration of a session by passing arguments to the constructor (see ```help slurm/slurm```).

## Usage - The mslurmApp

After completing the installation steps, open the App:

```matlab
mslurmApp
```

The App shows which jobs have been submitted to the cluster, which are running, and which have failed. The app is also key for debugging as it gives you easy access to everything that was written to the command line window during the execution of a job. After selecting a job, press Ctrl-O, and a file with that information will open in the Matlab editor.  If your job is still running, this view in the editor will **not** update automatically, but you can press Ctrl-P to retrieve the latest copy of the command window from the cluster. Browse around the App to see what other information it provides (whether all of this works will depend on the specifics of your SLURM installation).

The app uses the preferences you set with `mslurm.install`. If you want to connect to a different cluster, or use  settings that are different from your saved preferences, you can create a mslurm object and open the app with that object. For instance:

```matlab
cls =mslurm('host','hpc.fast.edu','keyfile','my_id_rsa')
mslurmApp(cls)
```

## Usage - Creating and submitting jobs

To submit jobs, you first create an mslurm object (this one uses the saved preferences):

```matlab
cls = mslurm
```

The jobs posted to SLURM in each session typically share several parameters. You set these by assigning values to the mslurm object.

- **startupDirectory:** The folder where Matlab will start (this could be a place where your startup.m and pathdef.m live).
- **workingDirectory:** The folder where the jobs will execute. If left unspecified (""), this will use a subfolder of the remoteStorage folder created spefically for the job.
- **addPath:** A string array specifying the path(s) that should be included on the cluster. This  will be passed to addpath() on the cluster.
- **sbatchOptions:** A cell array of parameter/value pairs that are passed verbatim to the slurm sbatch command. See the [man page for sbatch for all options](https://slurm.schedmd.com/sbatch.html). These options are key to requesting the right kind of compute nodes for your job.
- **runOptions:** A string that is passed verbatim to the ```srun``` command. You probably do not need this, but see the [man page](https://slurm.schedmd.com/srun.html) for valid options.
- **env:** A string array with environment variables. These are read from the environment on the client and set on the cluster. To set a specific value that differs from the value set on the client, use "VAR=VALUE".

For example:

```matlab
c.addPath = "/home/bart/tools"; % Make sure that my tools folder is  accessible on the cluster. 
c.sbatchOptions = {'mem','32GB','time',30}; %Request workers with at least 32GB of memory and a wall time of 30 minutes. 
c.env = ["HOST" "USER" "HOME=/home/joe"] % Read the HOST and USER environment variables from the client, but set the HOME variable to /home/joe.
```

## Usage

See demos/tutorial.mlx for walktrough of various susage scenarios.

## Extending the toolbox

The slurm.sbatch function is the low-level workhorse that submits jobs to SLURM. You can call this directly from your code to make SLURM do anything you want. The mslurm.remote function (the main workhorse that can be used for many scenarios; see demos/tutorial.mlx)  shows one way of simplifying the calls, but ultimately calls nslurm.sbatch. You can use mslurm.remote or create a new wrapper around mslurm.sbatch that better suits your workflow.

[![DOI](https://zenodo.org/badge/93510696.svg)](https://zenodo.org/badge/latestdoi/93510696)
