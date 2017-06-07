# mslurm
Toolbox to send Matlab jobs from your local machine (the client) to an HPC cluster running the SLURM scheduler (the server).

Installation:
Server side
Make sure the command 'matlab' on the HPC cluster's command line starts Matlab. If it doesn't, add the path to .bashrc. For instance,
by adding the following line to .bashrc
PATH=$PATH:/opt/sw/packages/MATLAB/R2016a/bin/: 
(but of course your installation path for Matlab on the cluster can vary).


This package depends on the SSH2 package (David Freedman); install it first:
Clone the SSH2 repository (https://github.com/davidfreedman/matlab-ssh2.git) to the client and follow its instructions to set it up. 

Clone the mslurm repository to the client and to the server.
Add the mslurm directory to the Matlab search path on both client and server

The GUI additionally depends on the findjobj tool (Yair Altman). Download it from the file exchange:
https://nl.mathworks.com/matlabcentral/fileexchange/14317-findjobj-find-java-handles-of-matlab-graphic-objects
and add it to your Matlab install on the client. 
