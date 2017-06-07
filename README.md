# mslurm

This is a small toolbox to send Matlab jobs from your local machine (the client) to an HPC cluster running the SLURM scheduler (the server). As long as you have unlimited licenses to run Matlab on the cluster, this allows you to run man jobs in parallel, without the distributed computing server license. This only works for "dumb" parallelism though; messaging between jobs is not used. 

Installation:
*Server side*
Make sure the command 'matlab' on the HPC cluster's command line starts Matlab. If it doesn't, add the path to  .bashrc. For instance, by adding the following line:
PATH=$PATH:/opt/sw/packages/MATLAB/R2016a/bin/: 
(but of course your installation path for Matlab on the cluster can vary).

Clone the mslurm repository to the server.
Add the mslurm directory to the Matlab search path on the server

*Client Side*
Clone the mslurm repository to the client .
Add the mslurm directory to the Matlab search path on the client.

This package depends on the SSH2 package (David Freedman); install it:
Clone the SSH2 repository (https://github.com/davidfreedman/matlab-ssh2.git) to the client and follow its instructions to set it up. 

The GUI additionally depends on the findjobj tool (Yair Altman). Download it from the file exchange:
https://nl.mathworks.com/matlabcentral/fileexchange/14317-findjobj-find-java-handles-of-matlab-graphic-objects
and add it to your Matlab install on the client. 
